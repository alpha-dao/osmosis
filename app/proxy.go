package app

import (
	"bytes"
	"database/sql"
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	appparams "github.com/osmosis-labs/osmosis/v12/app/params"
	"net/http"
	"os"
	"strings"
	"time"

	"encoding/json"
	"github.com/gogo/protobuf/jsonpb"
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/types"
	"log"
	"sync"
)

const (
	tableTxResults              = "osmosis_tx_results"
	tableTxMsgs                 = "osmosis_tx_messages"
	tableAssetReceiveEvent      = "asset_receive_event"
	tableKeplrNotificationEvent = "keplr_notification_event"
	driverName                  = "postgres"
)

// EventSink is an indexer backend providing the tx/block index services.  This
// implementation stores records in a PostgreSQL database using the schema
// defined in state/indexer/sink/psql/schema.sql.
type EventSink struct {
	store      *sql.DB
	chainID    string
	config     appparams.EncodingConfig
	eventIndex bool
	rwMutex    *sync.RWMutex
	hMap       map[int64]time.Time
}

type KeplrAssetReceivedPayload struct {
	ChainID    string `json:"chain_id"`
	FromSigner string `json:"from_signer"`
	Denom      string `json:"denom"`
	Amount     string `json:"amount"`
}

type KeplrAssetReceivedUniquePayload struct {
	ChainID     string `json:"chain_id"`
	Signer      string `json:"signer"`
	BlockHeight int64  `json:"block_height"`
	TxIndex     uint32 `json:"tx_index"`
	MsgIndex    int    `json:"msg_index"`
}

func (p KeplrAssetReceivedPayload) String() string {
	b, _ := json.Marshal(p)
	return string(b)
}

func (p KeplrAssetReceivedUniquePayload) String() string {
	b, _ := json.Marshal(p)
	return string(b)
}

var (
	jsonpbMarshaller = jsonpb.Marshaler{}
)

// NewEventSink constructs an event sink associated with the PostgreSQL
// database specified by connStr. Events written to the sink are attributed to
// the specified chainID.
func NewEventSink(connStr, chainID string, config appparams.EncodingConfig, eventIndex bool) (*EventSink, error) {
	db, err := sql.Open(driverName, connStr)
	if err != nil {
		return nil, err
	}

	rwmutex := new(sync.RWMutex)

	return &EventSink{
		store:      db,
		chainID:    chainID,
		config:     config,
		eventIndex: eventIndex,
		rwMutex:    rwmutex,
		hMap:       map[int64]time.Time{},
	}, nil
}

// DB returns the underlying Postgres connection used by the sink.
// This is exported to support testing.
func (es *EventSink) DB() *sql.DB { return es.store }

// runInTransaction executes query in a fresh database transaction.
// If query reports an error, the transaction is rolled back and the
// error from query is reported to the caller.
// Otherwise, the result of committing the transaction is returned.
func runInTransaction(db *sql.DB, query func(*sql.Tx) error) error {
	dbtx, err := db.Begin()
	if err != nil {
		return err
	}
	if err := query(dbtx); err != nil {
		_ = dbtx.Rollback() // report the initial error, not the rollback
		return err
	}
	return dbtx.Commit()
}

// queryWithID executes the specified SQL query with the given arguments,
// expecting a single-row, single-column result containing an ID. If the query
// succeeds, the ID from the result is returned.
func queryWithID(tx *sql.Tx, query string, args ...interface{}) (uint32, error) {
	var id uint32
	if err := tx.QueryRow(query, args...).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// insertEvents inserts a slice of events and any indexed attributes of those
// events into the database associated with dbtx.
//
// If txID > 0, the event is attributed to the Tendermint transaction with that
// ID; otherwise it is recorded as a block event.
func insertEvents(dbtx *sql.Tx, blockID, txID uint32, evts []abci.Event) error {
	// Populate the transaction ID field iff one is defined (> 0).
	// TODO: later
	return nil
}

// makeIndexedEvent constructs an event from the specified composite key and
// value. If the key has the form "type.name", the event will have a single
// attribute with that name and the value; otherwise the event will have only
// a type and no attributes.
func makeIndexedEvent(compositeKey, value string) abci.Event {
	i := strings.Index(compositeKey, ".")
	if i < 0 {
		return abci.Event{Type: compositeKey}
	}
	return abci.Event{Type: compositeKey[:i], Attributes: []abci.EventAttribute{
		{Key: []byte(compositeKey[i+1:]), Value: []byte(value), Index: true},
	}}
}

// IndexBlockEvents indexes the specified block header, part of the
// indexer.EventSink interface.
func (es *EventSink) IndexBlockEvents(h types.EventDataNewBlockHeader) error {
	//ts := time.Now().UTC()
	//es.rwMutex.Lock()
	es.hMap[h.Header.Height] = h.Header.Time
	//es.rwMutex.Unlock()
	return nil
	/*return runInTransaction(es.store, func(dbtx *sql.Tx) error {
			// Add the block to the blocks table and report back its row ID for use
			// in indexing the events for the block.
			blockID, err := queryWithID(dbtx, `
	INSERT INTO `+tableBlocks+` (height, chain_id, created_at)
	  VALUES ($1, $2, $3)
	  ON CONFLICT DO NOTHING
	  RETURNING rowid;
	`, h.Header.Height, es.chainID, h.Header.Time)
			if err == sql.ErrNoRows {
				return nil // we already saw this block; quietly succeed
			} else if err != nil {
				return fmt.Errorf("indexing block header: %w", err)
			}

			if es.eventIndex == false {
				return nil
			}
			// Insert the special block meta-event for height.
			if err := insertEvents(dbtx, blockID, 0, []abci.Event{
				makeIndexedEvent(types.BlockHeightKey, fmt.Sprint(h.Header.Height)),
			}); err != nil {
				return fmt.Errorf("block meta-events: %w", err)
			}
			// Insert all the block events. Order is important here,
			if err := insertEvents(dbtx, blockID, 0, h.ResultBeginBlock.Events); err != nil {
				return fmt.Errorf("begin-block events: %w", err)
			}
			if err := insertEvents(dbtx, blockID, 0, h.ResultEndBlock.Events); err != nil {
				return fmt.Errorf("end-block events: %w", err)
			}
			return nil
		})*/
}

func (es *EventSink) IndexTxEvents(txrs []*abci.TxResult) error {
	if len(txrs) == 0 {
		return nil
	}
	//es.rwMutex.RLock()
	ts := es.hMap[txrs[0].Height]
	//es.rwMutex.RUnlock()

	for _, txr := range txrs {
		// Encode the result message in protobuf wire format for indexing.
		//resultData, err := proto.Marshal(txr)
		cosmosTx, err := es.config.TxConfig.TxDecoder()(txr.Tx)
		codespace, code, info, gasWanted, gasUsed := txr.Result.Codespace, txr.Result.Code, txr.Result.Info, txr.Result.GasWanted, txr.Result.GasUsed
		if err != nil {
			return fmt.Errorf("1 marshaling tx_result: %w", err)
		}
		resultString, err := es.config.TxConfig.TxJSONEncoder()(cosmosTx)
		if err != nil {
			return fmt.Errorf("2 marshaling tx_result: %w", err)
		}

		// Index the hash of the underlying transaction as a hex string.
		txHash := fmt.Sprintf("%X", types.Tx(txr.Tx).Hash())

		if err := runInTransaction(es.store, func(dbtx *sql.Tx) error {
			// Find the block associated with this transaction. The block header
			// must have been indexed prior to the transactions belonging to it

			// Insert a record for this tx_result and capture its ID for indexing events.
			_, err := queryWithID(dbtx, `
INSERT INTO `+tableTxResults+` (block_height, tx_index, created_at, tx_hash, tx_result, code, codespace, gas_used, gas_wanted, info)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
  ON CONFLICT DO NOTHING;
`, txr.Height, txr.Index, ts, txHash, resultString, code, codespace, gasUsed, gasWanted, info)
			if err == sql.ErrNoRows {
			} else if err != nil {
				return fmt.Errorf("indexing tx_result: %w", err)
			}

			//Insert Msgs
			for i, msg := range cosmosTx.GetMsgs() {
				err = indexMsg(msg, dbtx, es.DB(), i, txr, code, es.chainID, ts)
				if err != nil {
					return err
				}
			}

			if es.eventIndex == false {
				return nil
			}
			// Insert the special transaction meta-events for hash and height.

			return nil

		}); err != nil {
			return err
		}
	}
	return nil
}

func indexMsg(msg sdk.Msg, dbtx *sql.Tx, db *sql.DB, i int, txr *abci.TxResult, code uint32, chainID string, ts time.Time) error {
	for _, signer := range msg.GetSigners() {
		msgString, err := jsonpbMarshaller.MarshalToString(msg)
		if err != nil {
			return fmt.Errorf("indexing msg: %w", err)
		}
		msgType := sdk.MsgTypeURL(msg)

		_, err = queryWithID(dbtx, `
INSERT INTO `+tableTxMsgs+` (block_height, tx_index, msg_index, signer, msg_string, type, code)
  VALUES ($1, $2, $3, $4, $5, $6, $7)
  ON CONFLICT DO NOTHING;
`, txr.Height, txr.Index, i, signer.String(), msgString, msgType, code)

		if err == sql.ErrNoRows {
			// we've already inject this transaction; quietly succeed
		} else if err != nil {
			return fmt.Errorf("indexing msg: %w", err)
		}

		//index for asset receive!
		if msgType == "/cosmos.bank.v1beta1.MsgSend" {
			sendMsg, ok := msg.(*banktypes.MsgSend)
			if !ok {
				continue
			}

			//false receive message
			_, err = queryWithID(dbtx, `
INSERT INTO `+tableTxMsgs+` (block_height, tx_index, msg_index, signer, msg_string, type, code)
  VALUES ($1, $2, $3, $4, $5, $6, $7)
  ON CONFLICT DO NOTHING;
`, txr.Height, txr.Index, i, sendMsg.ToAddress, msgString, "/manythings.bank.v1beta1.MsgReceive", code)

			for _, coin := range sendMsg.Amount {
				_, err = queryWithID(dbtx, `
INSERT INTO `+tableAssetReceiveEvent+` (signer, created_at, chain_id, from_signer, denom, amount)
  VALUES($1, $2, $3, $4, $5, $6)
  ON CONFLICT DO NOTHING
  RETURNING id;
`, sendMsg.ToAddress, ts, chainID, sendMsg.FromAddress, coin.Denom, coin.Amount.String())

				//new asset received event v2
				payload := KeplrAssetReceivedPayload{
					chainID,
					signer.String(),
					coin.Denom,
					coin.Amount.String(),
				}
				uniquePayload := KeplrAssetReceivedUniquePayload{
					chainID,
					signer.String(),
					txr.Height,
					txr.Index,
					i,
				}
				if code != 0 {
					_, err = queryWithID(dbtx, `
INSERT INTO `+tableKeplrNotificationEvent+` (signer, notification_type, chain_id, payload, unique_payload)
  VALUES($1, $2, $3, $4, $5);
`, sendMsg.ToAddress, "asset_received", chainID, payload.String(), uniquePayload.String())

					go sendFcmMessage(db, payload, signer.String())
				}
			}
		}
	}
	return nil
}

func sendFcmMessage(db *sql.DB, payload KeplrAssetReceivedPayload, signer string) {
	//GET TOKEN
	var cosmosSigner string
	err := db.QueryRow("SELECT cosmoshub_signer FROM keplr_signer_cosmos_identity WHERE signer = $1", signer).Scan(&cosmosSigner)

	if err != nil {
		log.New(os.Stderr, "", 0).Println(err)
	}

	var webToken, mobileToken string
	err = db.QueryRow("SELECT web_token, mobile_token FROM keplr_fcm_token_status WHERE cosmoshub_signer = $1", cosmosSigner).Scan(&webToken, &mobileToken)

	tokens := []string{}
	if webToken != "" {
		tokens = append(tokens, webToken)
	}
	if mobileToken != "" {
		tokens = append(tokens, mobileToken)
	}

	type KeplrFCMAssetReceivedPayload struct {
		Token   string                    `json:"token"`
		Payload KeplrAssetReceivedPayload `json:"payload"`
	}

	for _, t := range tokens {
		p := KeplrFCMAssetReceivedPayload{
			Token:   t,
			Payload: payload,
		}
		payloadBytes, _ := json.Marshal(p)
		buff := bytes.NewBuffer(payloadBytes)
		fcmEndpoint := os.Getenv("FCM_ENDPOINT")
		_, err := http.Post(fcmEndpoint, "application/json", buff)
		if err != nil {
			log.New(os.Stderr, "", 0).Println(err)
		}
	}
}
