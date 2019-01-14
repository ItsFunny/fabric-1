package tendermintpbft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric/orderer/consensus"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/version"
	"strconv"
	"time"
	dbm "github.com/tendermint/tendermint/libs/db"
)

var (
	stateKey        = []byte("stateKey")
	ProtocolVersion version.Protocol = 0x1
)

const (
	CodeTypeOK            uint32 = 0
	CodeTypeEncodingError uint32 = 1
	CodeTypeBadNonce      uint32 = 2
	CodeTypeUnauthorized  uint32 = 3
	CodeTypeUnknownError  uint32 = 4
)


//---------------------------------------------------


type FabricStoreApplication struct {
	state State
	exitChan chan struct{}
	sendChan chan *message
	types.BaseApplication
	chainSupports  map[string]consensus.ConsenterSupport
}
type State struct {
	db      dbm.DB
	Size    int64  `json:"size"`
	Height  int64  `json:"height"`
	AppHash []byte `json:"app_hash"`
}

func loadState(db dbm.DB) State {
	stateBytes := db.Get(stateKey)
	var state State
	if len(stateBytes) != 0 {
		err := json.Unmarshal(stateBytes, &state)
		if err != nil {
			panic(err)
		}
	}
	state.db = db
	return state
}
func saveState(state State) {
	stateBytes, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	state.db.Set(stateKey, stateBytes)
}

func NewFabricStoreApplication(dbDir string) *FabricStoreApplication {
	name := "fabric_tm_state"
	fmt.Println(name)
	fmt.Println(dbDir)
	db, err := dbm.NewGoLevelDB(name, dbDir)
	if err != nil {
		panic(err)
	}

	state := loadState(db)

	app := &FabricStoreApplication{state:state,sendChan:make(chan *message),exitChan:make(chan struct{}),chainSupports:make(map[string]consensus.ConsenterSupport)}
	return app
}

func  (app *FabricStoreApplication) addChain(ch consensus.ConsenterSupport){
	app.chainSupports[ch.ChainID()] = ch
}
func  (app *FabricStoreApplication) start(){
	go app.main()
}

func (app *FabricStoreApplication) errored() <-chan struct{} {
	return app.exitChan
}
func (app *FabricStoreApplication) halt() {
	select {
	case <-app.exitChan:
		// Allow multiple halts without panic
	default:
		close(app.exitChan)
	}
}
func (app *FabricStoreApplication) Info(req types.RequestInfo) (resInfo types.ResponseInfo) {
	return types.ResponseInfo{
		Data:       fmt.Sprintf("{\"size\":%v}", app.state.Size),
		Version:    version.ABCIVersion,
		AppVersion: ProtocolVersion.Uint64(),
		LastBlockHeight: app.state.Height,
		LastBlockAppHash: app.state.AppHash,
	}

}
func getKey(app *FabricStoreApplication,msg  *message)(string){
	return string(msg.ChainId)+":"+ strconv.Itoa(int(app.chainSupports[string(msg.ChainId)].Height())+1)
}
// tx is either "key=value" or just arbitrary bytes
func (app *FabricStoreApplication) DeliverTx(tx []byte) types.ResponseDeliverTx {
	fmt.Println("come in DeliverTx。。。")
	//fmt.Println("tx:"+string(tx))
	msg := &message{}
	newBuffer :=bytes.NewBuffer(tx)
	dec := gob.NewDecoder(newBuffer)
	err := dec.Decode(&msg)
	if err != nil {
		logger.Error("gob.Decoder err:"+err.Error())
		return types.ResponseDeliverTx{Code:CodeTypeEncodingError}
	}
	app.sendChan <- msg

	key := getKey(app,msg)

	app.state.db.Set([]byte(key), tx)

	app.state.Size += 1
 	return types.ResponseDeliverTx{Code: CodeTypeOK}
}
func (app *FabricStoreApplication) Commit() types.ResponseCommit {
	// Using a memdb - just return the big endian size of the db
	appHash := make([]byte, 8)
	binary.PutVarint(appHash, app.state.Size)
	app.state.AppHash = appHash
	app.state.Height += 1
	saveState(app.state)
	return types.ResponseCommit{Data: appHash}
}

func (app *FabricStoreApplication) main() {
	var timer <-chan time.Time
	var err error

	for {

		err = nil
		select {
		case msg := <-app.sendChan:
			support :=  app.chainSupports[string(msg.ChainId)]
			seq := support.Sequence()
			if msg.ConfigMsg == nil {
				// NormalMsg
				fmt.Println("come in NormalMsg。。。")
				if msg.ConfigSeq < seq {
					_, err = support.ProcessNormalMsg(msg.NormalMsg)
					if err != nil {
						logger.Error("Discarding bad normal message: %s", err)
						continue
					}
				}
				batches, pending := support.BlockCutter().Ordered(msg.NormalMsg)

				for _, batch := range batches {
					block := support.CreateNextBlock(batch)
					support.WriteBlock(block, nil)
				}

				switch {
				case timer != nil && !pending:
					// Timer is already running but there are no messages pending, stop the timer
					timer = nil
				case timer == nil && pending:
					// Timer is not already running and there are messages pending, so start it
					timer = time.After(support.SharedConfig().BatchTimeout())
					logger.Info("Just began %s batch timer", support.SharedConfig().BatchTimeout().String())
				default:
					// Do nothing when:
					// 1. Timer is already running and there are messages pending
					// 2. Timer is not set and there are no messages pending
				}

			} else {
				// ConfigMsg
				fmt.Println("come in ConfigMsg。。。")
				fmt.Println("msg.ConfigMsg:"+msg.ConfigMsg.String())
				//fmt.Println("payload.Header:"+string(msg.ConfigMsg.Payload))
				payload,err:= utils.UnmarshalPayload(msg.ConfigMsg.Payload)
				fmt.Println("payload.Header:"+payload.Header.String())
				if msg.ConfigSeq < seq {
					msg.ConfigMsg, _, err = support.ProcessConfigMsg(msg.ConfigMsg)
					if err != nil {
						logger.Error("Discarding bad config message: %s", err)
						continue
					}
				}
				batch := support.BlockCutter().Cut()
				if batch != nil {
					block := support.CreateNextBlock(batch)
					support.WriteBlock(block, nil)
				}

				block := support.CreateNextBlock([]*cb.Envelope{msg.ConfigMsg})
				support.WriteConfigBlock(block, nil)
				timer = nil
			}
		//case <-timer:
		//	//clear the timer
		//	timer = nil
		//
		//	batch := app.support.BlockCutter().Cut()
		//	if len(batch) == 0 {
		//		logger.Info("Batch timer expired with no pending requests, this might indicate a bug")
		//		continue
		//	}
		//	logger.Debug("Batch timer expired, creating block")
		//	block := app.support.CreateNextBlock(batch)
		//	app.support.WriteBlock(block, nil)
		case <-app.exitChan:
			logger.Debug("Exiting")
			return
		}
	}
}