/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tendermintpbft

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/hyperledger/fabric/orderer/consensus"
	. "github.com/hyperledger/fabric/orderer/consensus/tendermintpbft/types"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/tendermint/go-amino"
	bc "github.com/tendermint/tendermint/blockchain"
	cfg "github.com/tendermint/tendermint/config"
	cs "github.com/tendermint/tendermint/consensus"
	"github.com/tendermint/tendermint/crypto/encoding/amino"
	cmn "github.com/tendermint/tendermint/libs/common"
	dbm "github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/p2p/pex"
	"github.com/tendermint/tendermint/privval"
	rpccore "github.com/tendermint/tendermint/rpc/core"
	sm "github.com/tendermint/tendermint/state"
	"github.com/tendermint/tendermint/state/txindex"
	"github.com/tendermint/tendermint/state/txindex/kv"
	"github.com/tendermint/tendermint/state/txindex/null"
	"github.com/tendermint/tendermint/types"
)

var genesisDocKey = []byte("genesisDoc")


var cdc = amino.NewCodec()
var logger = log.NewTMLogger(log.NewSyncWriter(os.Stdout))
type transporter struct{
	transport *p2p.MultiplexTransport
	isListening bool
}
var trans = &transporter{transport:nil,isListening:false}
func init() {
	logger = log.NewFilter(logger,log.AllowDebug())
	cryptoAmino.RegisterAmino(cdc)
}


type consenter struct{}


type chain struct {
	support  consensus.ConsenterSupport
	exitChan chan struct{}

	Logger  log.Logger
	privValidator types.PrivValidator // local node's validator key

	// config
	config        *cfg.Config
	genesisDoc    *types.GenesisDoc   // initial validator set

	// network
	//transport   *p2p.MultiplexTransport
	sw          *p2p.Switch  // p2p connections
	addrBook    pex.AddrBook // known peers
	nodeInfo    p2p.NodeInfo
	nodeKey     *p2p.NodeKey // our node privkey
	isListening bool

	// services
	eventBus         *types.EventBus // pub/sub for services
	blockStore       *bc.BlockStore         // store the blockchain to disk
	bcReactor        *bc.BlockchainReactor  // for fast-syncing
	consensusState   *cs.ConsensusState     // latest consensus state
	consensusReactor *cs.ConsensusReactor   // for participating in the consensus
	rpcListeners     []net.Listener         // rpc servers
	txIndexer        txindex.TxIndexer
	indexerService   *txindex.IndexerService
	prometheusSrv    *http.Server
}



// DBContext specifies config information for loading a new DB.
type DBContext struct {
	ID     string
	Config *cfg.Config
}
func DefaultDBProvider(ctx *DBContext) (dbm.DB, error) {
	dbType := dbm.DBBackendType(ctx.Config.DBBackend)
	return dbm.NewDB(ctx.ID, dbType, ctx.Config.DBDir()), nil
}

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Order/Configure, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New() consensus.Consenter {
	return &consenter{}
}

// panics if failed to unmarshal bytes
func loadGenesisDoc(db dbm.DB) (*types.GenesisDoc, error) {
	bytes := db.Get(genesisDocKey)
	if len(bytes) == 0 {
		return nil, errors.New("Genesis doc not found")
	}
	var genDoc *types.GenesisDoc
	err := cdc.UnmarshalJSON(bytes, &genDoc)
	if err != nil {
		cmn.PanicCrisis(fmt.Sprintf("Failed to load genesis doc due to unmarshaling error: %v (bytes: %X)", err, bytes))
	}
	return genDoc, nil
}
func saveGenesisDoc(db dbm.DB, genDoc *types.GenesisDoc) {
	bytes, err := cdc.MarshalJSON(genDoc)
	if err != nil {
		cmn.PanicCrisis(fmt.Sprintf("Failed to save genesis doc due to marshaling error: %v", err))
	}
	db.SetSync(genesisDocKey, bytes)
}




func (solo *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	return newChain(support)
}


func newChain(support consensus.ConsenterSupport) (*chain,error) {
	fmt.Println("start new chain with chain name :" + support.ChainID())
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
	fmt.Println(1)
	err := vip.Unmarshal(config)

	if err != nil {
		return nil, err
	}

	config.SetRoot(config.RootDir)
	config.ChannelRootDir = config.RootDir +"/"+support.ChainID() +"/"

	cfg.EnsureRoot(config.RootDir)
	if err := config.ValidateBasic(); err != nil {
		return nil, fmt.Errorf("Error in config file: %v", err)
	}
	// Get BlockStore
	blockDB, err := DefaultDBProvider(&DBContext{"block", config})
	blockStore := bc.NewBlockStore(blockDB)
	// Get State

	stateDB, err := DefaultDBProvider(&DBContext{"state", config})
	if err != nil {
		return nil,err
	}
	fmt.Println(2)
	// Get genesis doc
	// TODO: move to state package?
	genDoc, err := loadGenesisDoc(stateDB)
	if err != nil {
		genDoc, err = types.GenesisDocFromFile(config.GenesisFile())
		if err != nil {
			return nil,err
		}
		// save genesis doc to prevent a certain class of user errors (e.g. when it
		// was changed, accidentally or not). Also good for audit trail.
		saveGenesisDoc(stateDB, genDoc)
	}
	fmt.Println(3)
	state, err := sm.LoadStateFromDBOrGenesisDoc(stateDB, genDoc)
	if err != nil {
		return nil,err
	}
	fmt.Println(3)
	state = sm.LoadState(stateDB)
	consensusLogger := logger.With("module", "consensus")
	fastSync := config.FastSync
	// Decide whether to fast-sync or not
	// We don't fast-sync when the only validator is us.
	privValidator := privval.LoadOrGenFilePV(config)


	if state.Validators.Size() == 1 {
		addr, _ := state.Validators.GetByIndex(0)
		if bytes.Equal(privValidator.GetAddress(), addr) {
			fastSync = false
		}
	}

	// Log whether this node is a validator or an observer
	if state.Validators.HasAddress(privValidator.GetAddress()) {
		consensusLogger.Info("This node is a validator", "addr", privValidator.GetAddress(), "pubKey", privValidator.GetPubKey())
	} else {
		consensusLogger.Info("This node is not a validator", "addr", privValidator.GetAddress(), "pubKey", privValidator.GetPubKey())
	}
	fmt.Println(5)

	//mtx := new(sync.Mutex)
	//proxyAppConnCon := abcicli.NewLocalClient(mtx, counter.NewCounterApplication(true))

	// mock the evidence pool
	evpool := sm.MockEvidencePool{}


	blockExecLogger := logger.With("module", "state")
	// make block executor for consensus and blockchain reactors to execute blocks
	blockExec := sm.NewBlockExecutor(
		stateDB,
		blockExecLogger,
		evpool,
	)

	//Make BlockchainReactor
	bcReactor := bc.NewBlockchainReactor(state.Copy(), blockExec,blockStore, fastSync)
	bcReactor.SetLogger(logger.With("module", "blockchain"))


	consensusState := cs.NewConsensusState(config.Consensus, state.Copy(), blockExec,blockStore, evpool)
	consensusState.SetLogger(consensusLogger)
	if privValidator != nil {
		consensusState.SetPrivValidator(privValidator)
	}


	//add by vito.he we may  not need catch up blocks because we have fabric! cool!
	consensusReactor := cs.NewConsensusReactor(consensusState, false)
	consensusReactor.SetLogger(consensusLogger)

	eventBus := types.NewEventBus()
	eventBus.SetLogger(logger.With("module", "events"))

	// services which will be publishing and/or subscribing for messages (events)
	// consensusReactor will set it on consensusState and blockExecutor
	consensusReactor.SetEventBus(eventBus)


	// Transaction indexing
	var txIndexer txindex.TxIndexer
	switch config.TxIndex.Indexer {
	case "kv":
		store, err := DefaultDBProvider(&DBContext{"tx_index", config})
		if err != nil {
			return nil,err
		}
		if config.TxIndex.IndexTags != "" {
			txIndexer = kv.NewTxIndex(store, kv.IndexTags(splitAndTrimEmpty(config.TxIndex.IndexTags, ",", " ")))
		} else if config.TxIndex.IndexAllTags {
			txIndexer = kv.NewTxIndex(store, kv.IndexAllTags())
		} else {
			txIndexer = kv.NewTxIndex(store)
		}
	default:
		txIndexer = &null.TxIndex{}
	}

	indexerService := txindex.NewIndexerService(txIndexer, eventBus)
	indexerService.SetLogger(logger.With("module", "txindex"))

	//读取node信息
	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	if err != nil {
		return nil,err
	}
	p2pLogger := logger.With("module", "p2p")
	nodeInfo, err := makeNodeInfo(
		config,
		nodeKey.ID(),
		txIndexer,
		genDoc.ChainID,
		p2p.NewProtocolVersion(
			1, // global
			state.Version.Consensus.Block,
			state.Version.Consensus.App,
		),
	)
	if err != nil {
		return nil,err
	}


	// Setup Transport.

		//add by vito.he change transport to a global varieble
		if trans.transport == nil{

	     mConnConfig := p2p.MConnConfig(config.P2P)
			trans.transport   = p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConnConfig)

		}

	 var connFilters = []p2p.ConnFilterFunc{}


	if !config.P2P.AllowDuplicateIP {
		connFilters = append(connFilters, p2p.ConnDuplicateIPFilter())
	}



	p2p.MultiplexTransportConnFilters(connFilters...)(trans.transport)

	// Setup Switch.
	sw := p2p.NewSwitch(
		support.ChainID(),
		config.P2P,
		trans.transport,
	)
	sw.SetLogger(p2pLogger)
	sw.AddReactor("CONSENSUS", consensusReactor)
	sw.AddReactor("BLOCKCHAIN", bcReactor)
	sw.SetNodeInfo(nodeInfo)
	sw.SetNodeKey(nodeKey)

	p2pLogger.Info("P2P Node ID", "ID", nodeKey.ID(), "file", config.NodeKeyFile())
	addrBook := pex.NewAddrBook(config.P2P.AddrBookFile(), config.P2P.AddrBookStrict)

	// Add ourselves to addrbook to prevent dialing ourselves
	addrBook.AddOurAddress(nodeInfo.NetAddress())

	addrBook.SetLogger(p2pLogger.With("book", config.P2P.AddrBookFile()))
	if config.P2P.PexReactor {
		// TODO persistent peers ? so we can have their DNS addrs saved
		pexReactor := pex.NewPEXReactor(addrBook,
			&pex.PEXReactorConfig{
				Seeds:    splitAndTrimEmpty(config.P2P.Seeds, ",", " "),
				SeedMode: config.P2P.SeedMode,
			})
		pexReactor.SetLogger(logger.With("module", "pex"))
		sw.AddReactor("PEX", pexReactor)
	}

	sw.SetAddrBook(addrBook)


	node := &chain{
		support:  support,
		config:        config,
		genesisDoc:    genDoc,
		privValidator: privValidator,
		//transport: transport,
		sw:        sw,
		nodeInfo:  nodeInfo,
		nodeKey:   nodeKey,
		addrBook:addrBook,
		blockStore:       blockStore,
		consensusState:   consensusState,
		consensusReactor: consensusReactor,
		txIndexer:        txIndexer,
		indexerService:   indexerService,
		eventBus:         eventBus,
		bcReactor:		  bcReactor,
	}
	node.Logger = logger
fmt.Println("return node successful!")
	 return node,nil
}

func (n *chain) Start() {
	fmt.Println("chain start")
	now := Now()
	genTime := n.genesisDoc.GenesisTime
	if genTime.After(now) {
		n.Logger.Info("Genesis time is in the future. Sleeping until then...", "genTime", genTime)
		time.Sleep(genTime.Sub(now))
	}

	err := n.eventBus.Start()
	if err != nil {
		//TODO
	}
	// Add private IDs to addrbook to block those peers being added
	n.addrBook.AddPrivateIDs(splitAndTrimEmpty(n.config.P2P.PrivatePeerIDs, ",", " "))

	// Start the RPC server before the P2P server
	// so we can eg. receive txs for the first block
	//if n.config.RPC.ListenAddress != "" {
	//	listeners, err := n.startRPC()
	//	if err != nil {
	//		//TODO
	//	}
	//	n.rpcListeners = listeners
	//}
	//
	//if n.config.Instrumentation.Prometheus &&
	//	n.config.Instrumentation.PrometheusListenAddr != "" {
	//	n.prometheusSrv = n.startPrometheusServer(n.config.Instrumentation.PrometheusListenAddr)
	//}

	// Start the transport.
	fmt.Println("n.config.P2P.ListenAddress:"+n.config.P2P.ListenAddress)
	addr, err := p2p.NewNetAddressStringWithOptionalID(n.config.P2P.ListenAddress)
	if err != nil {
		//TODO
	}
	trans.transport.AddSwitchAcceptC(n.sw.GetSwitchId())
	if  !trans.isListening{
		if err := trans.transport.Listen(*addr); err != nil {
			//TODO
			panic(errors.New("p2p listen start error!"))
		}else{
			trans.isListening = true
		}
	}


	n.isListening = true

	// Start the switch (the P2P server).
	err = n.sw.Start()
	if err != nil {
		//TODO
	}
	fmt.Println("p2p switch start")
	// Always connect to persistent peers
	if n.config.P2P.PersistentPeers != "" {
		err = n.sw.DialPeersAsync(n.addrBook, splitAndTrimEmpty(n.config.P2P.PersistentPeers, ",", " "), true)
		if err != nil {
			//TODO
		}
	}

	// start tx indexer
	 n.indexerService.Start()
	 go n.main()
}

func (n *chain)OnStop(){

	n.Logger.Info("Stopping Node")

	// first stop the non-reactor services
	n.eventBus.Stop()
	n.indexerService.Stop()

	// now stop the reactors
	// TODO: gracefully disconnect from peers.
	n.sw.Stop()

	//
	//if err := trans.transport.Close(); err != nil {
	//	n.Logger.Error("Error closing transport", "err", err)
	//}

	n.isListening = false

	// finally stop the listeners / external services
	for _, l := range n.rpcListeners {
		n.Logger.Info("Closing rpc listener", "listener", l)
		if err := l.Close(); err != nil {
			n.Logger.Error("Error closing listener", "listener", l, "err", err)
		}
	}

	if pvsc, ok := n.privValidator.(cmn.Service); ok {
		pvsc.Stop()
	}

	//if n.prometheusSrv != nil {
	//	if err := n.prometheusSrv.Shutdown(context.Background()); err != nil {
	//		// Error from closing listeners, or context timeout:
	//		n.Logger.Error("Prometheus HTTP server Shutdown", "err", err)
	//	}
	//}
}
func (ch *chain) Halt() {
	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

func (ch *chain) WaitReady() error {
	return nil
}

// Order accepts normal messages for ordering
func (ch *chain) Order(env *cb.Envelope, configSeq uint64) error {
	fmt.Println("come in Order")
	ch.consensusState.SendConsReqToPool(&Message{
		ConfigSeq: configSeq,
		NormalMsg: env,
	})
	return nil
}

// Configure accepts configuration update messages for ordering
func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {
	fmt.Println("come in config")
	ch.consensusState.SendConsReqToPool(&Message{
		ConfigSeq: configSeq,
		ConfigMsg: config,
	})
	return nil
	//select {
	//case <-ch.exitChan:
	//	return fmt.Errorf("Exiting")
	//}
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	return ch.exitChan
}

// splitAndTrimEmpty slices s into all subslices separated by sep and returns a
// slice of the string s with all leading and trailing Unicode code points
// contained in cutset removed. If sep is empty, SplitAndTrim splits after each
// UTF-8 sequence. First part is equivalent to strings.SplitN with a count of
// -1.  also filter out empty strings, only return non-empty strings.
func splitAndTrimEmpty(s, sep, cutset string) []string {
	if s == "" {
		return []string{}
	}

	spl := strings.Split(s, sep)
	nonEmptyStrings := make([]string, 0, len(spl))
	for i := 0; i < len(spl); i++ {
		element := strings.Trim(spl[i], cutset)
		if element != "" {
			nonEmptyStrings = append(nonEmptyStrings, element)
		}
	}
	return nonEmptyStrings
}

func makeNodeInfo(
	config *cfg.Config,
	nodeID p2p.ID,
	txIndexer txindex.TxIndexer,
	chainID string,
	protocolVersion p2p.ProtocolVersion,
) (p2p.NodeInfo, error) {
	txIndexerStatus := "on"
	if _, ok := txIndexer.(*null.TxIndex); ok {
		txIndexerStatus = "off"
	}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: protocolVersion,
		ID_:             nodeID,
		Network:         chainID,
		Version:         "1.0.0",
		Channels: []byte{
			bc.BlockchainChannel,
			cs.StateChannel, cs.DataChannel, cs.VoteChannel, cs.VoteSetBitsChannel,
			//mempl.MempoolChannel,
			//evidence.EvidenceChannel,
		},
		Moniker: config.Moniker,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    txIndexerStatus,
			//RPCAddress: config.RPC.ListenAddress,
		},
	}

	if config.P2P.PexReactor {
		nodeInfo.Channels = append(nodeInfo.Channels, pex.PexChannel)
	}

	lAddr := config.P2P.ExternalAddress

	if lAddr == "" {
		lAddr = config.P2P.ListenAddress
	}

	nodeInfo.ListenAddr = lAddr

	err := nodeInfo.Validate()
	return nodeInfo, err
}

// Now returns the current time in UTC with no monotonic component.
func Now() time.Time {
	return canonical(time.Now())
}

// Canonical returns UTC time with no monotonic component.
// Stripping the monotonic component is for time equality.
// See https://github.com/tendermint/tendermint/pull/2203#discussion_r215064334
func canonical(t time.Time) time.Time {
	return t.Round(0).UTC()
}


func (n *chain) ConfigureRPC() {
	rpccore.SetBlockStore(n.blockStore)
	rpccore.SetConsensusState(n.consensusState)
	rpccore.SetP2PPeers(n.sw)
	rpccore.SetP2PTransport(n)
	rpccore.SetPubKey(n.privValidator.GetPubKey())
	rpccore.SetGenesisDoc(n.genesisDoc)
	rpccore.SetTxIndexer(n.txIndexer)
	rpccore.SetConsensusReactor(n.consensusReactor)
	rpccore.SetEventBus(n.eventBus)
	rpccore.SetLogger(n.Logger.With("module", "rpc"))
}
//
//func (n *chain) startRPC() ([]net.Listener, error) {
//	n.ConfigureRPC()
//	listenAddrs := splitAndTrimEmpty(n.config.RPC.ListenAddress, ",", " ")
//	coreCodec := amino.NewCodec()
//	ctypes.RegisterAmino(coreCodec)
//
//	if n.config.RPC.Unsafe {
//		rpccore.AddUnsafeRoutes()
//	}
//
//	// we may expose the rpc over both a unix and tcp socket
//	listeners := make([]net.Listener, len(listenAddrs))
//	for i, listenAddr := range listenAddrs {
//		mux := http.NewServeMux()
//		rpcLogger := n.Logger.With("module", "rpc-server")
//		wm := rpcserver.NewWebsocketManager(rpccore.Routes, coreCodec, rpcserver.EventSubscriber(n.eventBus))
//		wm.SetLogger(rpcLogger.With("protocol", "websocket"))
//		mux.HandleFunc("/websocket", wm.WebsocketHandler)
//		rpcserver.RegisterRPCFuncs(mux, rpccore.Routes, coreCodec, rpcLogger)
//
//		listener, err := rpcserver.Listen(
//			listenAddr,
//			rpcserver.Config{MaxOpenConnections: n.config.RPC.MaxOpenConnections},
//		)
//		if err != nil {
//			return nil, err
//		}
//
//		var rootHandler http.Handler = mux
//		if n.config.RPC.IsCorsEnabled() {
//			corsMiddleware := cors.New(cors.Options{
//				AllowedOrigins: n.config.RPC.CORSAllowedOrigins,
//				AllowedMethods: n.config.RPC.CORSAllowedMethods,
//				AllowedHeaders: n.config.RPC.CORSAllowedHeaders,
//			})
//			rootHandler = corsMiddleware.Handler(mux)
//		}
//
//		go rpcserver.StartHTTPServer(
//			listener,
//			rootHandler,
//			rpcLogger,
//		)
//		listeners[i] = listener
//	}
//
//	// we expose a simplified api over grpc for convenience to app devs
//	grpcListenAddr := n.config.RPC.GRPCListenAddress
//	if grpcListenAddr != "" {
//		listener, err := rpcserver.Listen(
//			grpcListenAddr, rpcserver.Config{MaxOpenConnections: n.config.RPC.GRPCMaxOpenConnections})
//		if err != nil {
//			return nil, err
//		}
//		go grpccore.StartGRPCServer(listener)
//		listeners = append(listeners, listener)
//	}
//
//	return listeners, nil
//}


// startPrometheusServer starts a Prometheus HTTP server, listening for metrics
// collectors on addr.
//func (n *chain) startPrometheusServer(addr string) *http.Server {
//	srv := &http.Server{
//		Addr: addr,
//		Handler: promhttp.InstrumentMetricHandler(
//			prometheus.DefaultRegisterer, promhttp.HandlerFor(
//				prometheus.DefaultGatherer,
//				promhttp.HandlerOpts{MaxRequestsInFlight: n.config.Instrumentation.MaxOpenConnections},
//			),
//		),
//	}
//	go func() {
//		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
//			// Error starting or closing listener:
//			n.Logger.Error("Prometheus HTTP server ListenAndServe", "err", err)
//		}
//	}()
//	return srv
//}

func (n *chain) Listeners() []string {
	return []string{
		fmt.Sprintf("Listener(@%v)", n.config.P2P.ExternalAddress),
	}
}

func (n *chain) IsListening() bool {
	return n.isListening
}

// NodeInfo returns the Node's Info from the Switch.
func (n *chain) NodeInfo() p2p.NodeInfo {
	return n.nodeInfo
}


func (ch *chain) main() {
	var timer <-chan time.Time
	var err error

	for {
		seq := ch.support.Sequence()
		err = nil
		select {
		case msg := <-ch.consensusState.GetWaitCommitPool():
			if msg.ConfigMsg == nil {
				fmt.Println("receive pbft commitPoll normalMsg:"+msg.NormalMsg.String())
				// NormalMsg
				if msg.ConfigSeq < seq {
					_, err = ch.support.ProcessNormalMsg(msg.NormalMsg)
					if err != nil {
						logger.Info("Discarding bad normal message: %s", err)
						continue
					}
				}
				batches, pending := ch.support.BlockCutter().Ordered(msg.NormalMsg)

				for _, batch := range batches {
					block := ch.support.CreateNextBlock(batch)
					ch.support.WriteBlock(block, nil)
				}

				switch {
				case timer != nil && !pending:
					// Timer is already running but there are no messages pending, stop the timer
					timer = nil
				case timer == nil && pending:
					// Timer is not already running and there are messages pending, so start it
					timer = time.After(ch.support.SharedConfig().BatchTimeout())
					logger.Debug("Just began %s batch timer", ch.support.SharedConfig().BatchTimeout().String())
				default:
					// Do nothing when:
					// 1. Timer is already running and there are messages pending
					// 2. Timer is not set and there are no messages pending
				}

			} else {
				// ConfigMsg
				fmt.Println("receive  gpbft commitPoll configMsg:"+msg.ConfigMsg.String())

				if msg.ConfigSeq < seq {
					msg.ConfigMsg, _, err = ch.support.ProcessConfigMsg(msg.ConfigMsg)
					if err != nil {
						logger.Info("Discarding bad config message: %s", err)
						continue
					}
				}
				batch := ch.support.BlockCutter().Cut()
				if batch != nil {
					block := ch.support.CreateNextBlock(batch)
					ch.support.WriteBlock(block, nil)
				}

				block := ch.support.CreateNextBlock([]*cb.Envelope{msg.ConfigMsg})
				ch.support.WriteConfigBlock(block, nil)
				timer = nil
			}
		 fmt.Println("write blockchain SUCCESSFULLY!!!!")
		case <-timer:
			//clear the timer
			timer = nil

			batch := ch.support.BlockCutter().Cut()
			if len(batch) == 0 {
				logger.Info("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}
			logger.Debug("Batch timer expired, creating block")
			block := ch.support.CreateNextBlock(batch)
			ch.support.WriteBlock(block, nil)
		case <-ch.exitChan:
			logger.Debug("Exiting")
			return
		}
	}
}