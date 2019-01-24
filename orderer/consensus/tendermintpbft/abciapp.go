package tendermintpbft

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric/orderer/consensus"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/tendermint/tendermint/abci/types"
	dbm "github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/version"
	"strconv"
	"sync"
	"time"
)

var (
	stateKey        = []byte("stateKey")
	ProtocolVersion version.Protocol = 0x1
)

const (
	CodeTypeOK            uint32 = 0
	CodeTypeEncodingError uint32 = 1
	CodeTypeMsgInvalid       uint32 = 2
	CodeTypeUnauthorized  uint32 = 3
	CodeTypeUnknownError  uint32 = 4
)


//---------------------------------------------------


type queue struct{
	q *list.List
	sync.Mutex
}
func newQueue()(*queue){
	return &queue{q:list.New()}
}
func (q *queue)push(i *message){
	q.Lock()
	q.q.PushBack(i)
	q.Unlock()
}
func (q *queue)popAndDo(f func(envelope *message))(){
	q.Lock()
	for e := q.q.Front(); e != nil; e = e.Next() {
		f(e.Value.(*message))
	}
	//clear the list
	q.q.Init()
	q.Unlock()
}
func (q *queue)pop()(*message){
	q.Lock()
	e:= q.q.Front()
	q.q.Remove(e)
	q.Unlock()
	return e.Value.(*message)
}
func (q *queue)toSlice()([]*message){
	q.Lock()
	slice := make([]*message,q.q.Len())
	for e := q.q.Front(); e != nil; e = e.Next() {
		slice =append(slice, e.Value.(*message))
	}
	q.Unlock()
	return slice
}
func (q *queue)len()(int){
	return q.q.Len()
}
func (q *queue)clear(){
	q.q.Init()
}


type FabricStoreApplication struct {
	state State
	exitChan chan struct{}
	sendChan chan *message
	types.BaseApplication
	msgQ map[string]*queue
	chainSupports  map[string]consensus.ConsenterSupport
	timer map [string]<-chan time.Time
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

	app := &FabricStoreApplication{msgQ:make(map[string]*queue),state:state,sendChan:make(chan *message),exitChan:make(chan struct{}),chainSupports:make(map[string]consensus.ConsenterSupport),timer: make(map[string]<-chan time.Time)}
	return app
}

func  (app *FabricStoreApplication) addChain(ch consensus.ConsenterSupport){
	app.chainSupports[ch.ChainID()] = ch
	app.timer[ch.ChainID()] = nil
	app.msgQ[ch.ChainID()] = newQueue()
}
func  (app *FabricStoreApplication) start(){
	//go app.main()
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

func (app *FabricStoreApplication) CheckTx(tx []byte) types.ResponseCheckTx {

	msg := &message{}
	newBuffer :=bytes.NewBuffer(tx)
	dec := gob.NewDecoder(newBuffer)
	err := dec.Decode(&msg)
	if err != nil {
		logger.Error("gob.Decoder err:"+err.Error())
		return types.ResponseCheckTx{Code: CodeTypeMsgInvalid}
	}

   	support := app.chainSupports[string(msg.ChainId)]
   	//加tx有效性校验
	if msg.ConfigMsg == nil{

			 _,err = support.ProcessNormalMsg(msg.NormalMsg)
			if err != nil {
				logger.Error("Discarding bad normal message: %s", err)
				return types.ResponseCheckTx{Code: CodeTypeMsgInvalid}

		}
	}
   	//else{
	//		_, _, err = support.ProcessConfigMsg(msg.ConfigMsg)
	//		if err != nil {
	//			logger.Error("Discarding bad config message: %s", err)
	//			return types.ResponseCheckTx{Code: CodeTypeMsgInvalid}
	//		}
	//
	//}


	return types.ResponseCheckTx{Code: CodeTypeOK}
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
	//fmt.Println("come in DeliverTx。。。")
	//fmt.Println("tx:"+string(tx))
	msg := &message{}
	newBuffer :=bytes.NewBuffer(tx)
	dec := gob.NewDecoder(newBuffer)
	err := dec.Decode(&msg)
	if err != nil {
		logger.Error("gob.Decoder err:"+err.Error())
		return types.ResponseDeliverTx{Code:CodeTypeEncodingError}
	}
	//之所以在这里将tx交还给fabric，是因为fabric本身有批次的逻辑。什么时候真正出块还是由fabric来决定
	//这里tendermint的state只记录fabric的tx。fabric的追块，也只是通过tendermint传递tx。tm的块和fabric的块，概念不同。
	//这里弱化了tm中块的很多概念，tm中块起到的作用： 存储fabric的tx，用于fabric追块。
	//app.sendChan <- msg

	app.msgQ[string(msg.ChainId)].push(msg)
	
	key := getKey(app,msg)

	app.state.db.Set([]byte(key), tx)

	app.state.Size += 1
 	return types.ResponseDeliverTx{Code: CodeTypeOK}
}
func (app *FabricStoreApplication) Commit() types.ResponseCommit {
	// Using a memdb - just return the big endian size of the db
	for i,support :=range app.chainSupports{
		if app.msgQ[i].len()>0{
			var batches []*cb.Envelope
			app.msgQ[i].popAndDo(func(msg *message) {
				seq := support.Sequence()
				var err error
				if msg.ConfigMsg !=nil{
					logger.Info(support.ChainID()+"-create config block...")
					if msg.ConfigSeq < seq {
						msg.ConfigMsg, _, err = support.ProcessConfigMsg(msg.ConfigMsg)
						if err != nil {
							logger.Error("Discarding bad config message: %s", err)
							panic(err)
						}
					}
					block := support.CreateNextBlock([]*cb.Envelope{msg.ConfigMsg})
					support.WriteConfigBlock(block, nil)
				}else{
					batches = append(batches, msg.NormalMsg)
				}

			})

			if len(batches)>0{
				fmt.Println(support.ChainID()+"-create normal block...")
				block := support.CreateNextBlock(batches)
				support.WriteBlock(block, nil)
			}

		}
		
	}
	appHash := make([]byte, 8)
	binary.PutVarint(appHash, app.state.Size)
	app.state.AppHash = appHash
	app.state.Height += 1
	saveState(app.state)
	return types.ResponseCommit{Data: appHash}
}
//
//func (app *FabricStoreApplication) main() {
//
//	for {
//
//		select {
//		case msg := <-app.sendChan:
//			chainId := string(msg.ChainId)
//			support :=  app.chainSupports[chainId]
//			if msg.ConfigMsg == nil {
//				// NormalMsg
//				fmt.Println("come in NormalMsg。。。")
//
//				batches, pending := support.BlockCutter().Ordered(msg.NormalMsg)
//
//				for _, batch := range batches {
//					block := support.CreateNextBlock(batch)
//					support.WriteBlock(block, nil)
//				}
//
//				switch {
//				case app.timer[chainId] != nil && !pending:
//					// Timer is already running but there are no messages pending, stop the timer
//					app.timer[chainId] = nil
//				case app.timer[chainId] == nil && pending:
//					// Timer is not already running and there are messages pending, so start it
//					app.timer[chainId] = time.After(support.SharedConfig().BatchTimeout())
//					logger.Info("Just began %s batch timer", support.SharedConfig().BatchTimeout().String())
//				default:
//					// Do nothing when:
//					// 1. Timer is already running and there are messages pending
//					// 2. Timer is not set and there are no messages pending
//				}
//
//			} else {
//				// ConfigMsg
//				fmt.Println("come in ConfigMsg。。。")
//				var err error
//				msg.ConfigMsg, _, err = support.ProcessConfigMsg(msg.ConfigMsg)
//				if err != nil {
//					logger.Error("Discarding bad config message: %s", err)
//					continue
//				}
//				batch := support.BlockCutter().Cut()
//				if batch != nil {
//					block := support.CreateNextBlock(batch)
//					support.WriteBlock(block, nil)
//				}
//
//				block := support.CreateNextBlock([]*message{msg.ConfigMsg})
//				support.WriteConfigBlock(block, nil)
//				app.timer[chainId] = nil
//			}
//
//		case <-app.exitChan:
//			logger.Debug("Exiting")
//			return
//		}
//	}
//}