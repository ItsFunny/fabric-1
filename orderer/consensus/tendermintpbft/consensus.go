/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tendermintpbft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/spf13/viper"
	abci "github.com/tendermint/tendermint/abci/types"
	"os"

	"sync"

	"github.com/hyperledger/fabric/orderer/consensus"
	cb "github.com/hyperledger/fabric/protos/common"
	cfg "github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/node"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/privval"
	"github.com/tendermint/tendermint/proxy"
	"github.com/tendermint/tendermint/types"
)



var logger = log.NewTMLogger(log.NewSyncWriter(os.Stdout))

func init() {
	logger = log.NewFilter(logger,log.AllowDebug())
}

type consenter struct{}

var tmNode *tendermintNode = &tendermintNode{running:false,initialized:false,mut:sync.Mutex{}}


type tendermintNode struct{

	fabricStore      *FabricStoreApplication

	node *node.Node

	mut     sync.Mutex

	initialized bool

	running  bool


}

func (n *tendermintNode)nodeRunning()(bool){
	n.mut.Lock()
	defer n.mut.Unlock()

	return n.running
}

func (n *tendermintNode)nodeInitialized()(bool){
	n.mut.Lock()
	defer n.mut.Unlock()

	return n.initialized
}

func (n *tendermintNode)initialize(  support consensus.ConsenterSupport)(error){
	n.mut.Lock()
	defer n.mut.Unlock()
	//default config

	config := &cfg.Config{}
	viper.Reset()
	vip := viper.New()
	//vip.SetConfigFile("/Users/hunter/.tendermint/config/config.toml")

	if vip.BindEnv("TM_HOME") ==nil {
		configFile := vip.Get("TM_HOME").(string)+"config/config.toml"
		vip.SetConfigFile(configFile)
		vip.SetConfigType("toml")
		config.RootDir = vip.Get("TM_HOME").(string)
	}



	if err := vip.ReadInConfig(); err != nil {
		fmt.Println("Can't read config:", err)
		config = cfg.DefaultConfig()
	}
	err := vip.Unmarshal(config)

	if err != nil {
		return  err
	}

	config.SetRoot(config.RootDir)

	cfg.EnsureRoot(config.RootDir)
	if err := config.ValidateBasic(); err != nil {
		return fmt.Errorf("Error in config file: %v", err)
	}

	fabricStore := NewFabricStoreApplication(config.DBDir())

	clientCreator := proxy.NewLocalClientCreator(fabricStore)
	privValidator := privval.LoadOrGenFilePV(config.PrivValidatorFile())
	//读取node信息
	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	if err != nil {
		return err
	}
	n.node ,err = node.NewNode(config,privValidator,nodeKey,clientCreator,node.DefaultGenesisDocProviderFunc(config),
		node.DefaultDBProvider,node.DefaultMetricsProvider(config.Instrumentation),logger)
	if err != nil {
		return err
	}
	n.initialized = true
	n.fabricStore = fabricStore


	return nil
}

func (n *tendermintNode)start(){
	n.mut.Lock()
	defer n.mut.Unlock()
	if err:=n.node.Start();err ==nil{
		n.running = true
		n.fabricStore.start()
	}else{
		logger.Error("tm node start error!"+err.Error())
	}
}
func (n *tendermintNode)stop(){
	n.mut.Lock()
	defer n.mut.Unlock()
	if n.node.Stop() ==nil{
		n.running = false
	}else{
		logger.Error("tm node stop error!")
	}
}

type chain struct {

	tmNode   *tendermintNode

	support          consensus.ConsenterSupport
}

type baseMessage interface{
	validateBasic()error
}
type message struct {
	ConfigSeq uint64
	NormalMsg *cb.Envelope
	ConfigMsg *cb.Envelope
	ChainId   []byte
}
func (msg message)validateBasic()(error){
	return nil
}



// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Order/Configure, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New() consensus.Consenter {
	return &consenter{}
}




func (solo *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	return newChain(support)
}


func newChain(support consensus.ConsenterSupport) (*chain,error) {
	fmt.Println("start new chain with chain name :" + support.ChainID())


	if !tmNode.nodeInitialized(){
		if err :=tmNode.initialize(support);err!=nil{
			return nil,err
		}
	}
	tmNode.fabricStore.addChain(support)
	return &chain{tmNode:tmNode,support:support},nil
}


func (n *chain) Start() {
	if n.tmNode.nodeInitialized()&&!n.tmNode.nodeRunning(){
		n.tmNode.start()

	}
}

func (n *chain)OnStop(){
	if n.tmNode.nodeInitialized()&&n.tmNode.nodeRunning(){
		n.tmNode.stop()
	}
}
func (ch *chain) Halt() {
	ch.tmNode.fabricStore.halt()
}

func (ch *chain) WaitReady() error {
	return nil
}

// Order accepts normal messages for ordering
func (ch *chain) Order(env *cb.Envelope, ConfigSeq uint64) error {
	tx:= types.Tx{}
	var byteBuffer bytes.Buffer
	enc := gob.NewEncoder(&byteBuffer)

	err := enc.Encode(&message{
		ConfigSeq: ConfigSeq,
		NormalMsg: env,
		ChainId:[]byte(ch.support.ChainID()),
	})
	if err != nil {
		return errors.New(fmt.Sprint("encode error:", err))
	}
	tx =  byteBuffer.Bytes()

	err = ch.tmNode.node.MempoolReactor().Mempool.CheckTx(tx, func(res *abci.Response) {
		//TODO
	})
	return nil
}

// Configure accepts configuration update messages for ordering
func (ch *chain) Configure(config *cb.Envelope, ConfigSeq uint64) error {

	tx:= types.Tx{}

	var byteBuffer bytes.Buffer
	enc := gob.NewEncoder(&byteBuffer)
	err := enc.Encode(&message{
		ConfigSeq: ConfigSeq,
		ConfigMsg: config,
		ChainId:[]byte(ch.support.ChainID()),
	})

	if err != nil {
		return errors.New(fmt.Sprint("encode error:", err))
	}
	tx =  byteBuffer.Bytes()

	err = ch.tmNode.node.MempoolReactor().Mempool.CheckTx(tx, func(res *abci.Response) {
		//TODO
	})
	return nil
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	return ch.tmNode.fabricStore.errored()
}
