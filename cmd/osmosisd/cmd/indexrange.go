package cmd

// DONTCOVER

import (
	"database/sql"
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	app "github.com/osmosis-labs/osmosis/v10/app"
	"github.com/spf13/cobra"
	"github.com/syndtr/goleveldb/leveldb/opt"
	abcitypes "github.com/tendermint/tendermint/abci/types"
	tmstate "github.com/tendermint/tendermint/state"
	tmstore "github.com/tendermint/tendermint/store"
	"github.com/tendermint/tendermint/types"
	tmdb "github.com/tendermint/tm-db"
	"os"
	"os/exec"
	"strconv"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/tendermint/tendermint/config"
)

const (
	irStartHeight = "irStartHeight"
	irEndHeight   = "irEndHeight"
	irConnStr     = "irConnStr"
	irNumThread   = "irNumThread"
	irIndexEvent  = "irIndexEvent"
	irDesc        = "irDesc"
)

// get cmd to convert any bech32 address to an osmo prefix.
func indexRange() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "indexrange",
		Short: "Example osmosisd indexrange -s 1 -e 1000, which would index block data from s height to e height.",
		Long:  "Index range options indexes blocks as file at given range from blockstore.db. One needs to shut down chain before running indexrange.",
		RunE: func(cmd *cobra.Command, args []string) error {
			irStartHeightFlag, err := cmd.Flags().GetString(irStartHeight)
			if err != nil {
				return err
			}

			irEndHeightFlag, err := cmd.Flags().GetString(irEndHeight)
			if err != nil {
				return err
			}

			/*irConnStrFlag, err := cmd.Flags().GetString(irConnStr)
			if err != nil {
				return err
			}

			irNumThreadFlag, err := cmd.Flags().GetString(irNumThread)
			if err != nil {
				return err
			}

			irIndexEventFlag, err := cmd.Flags().GetString(irIndexEvent)
			if err != nil {
				return err
			}

			irDescFlag, err := cmd.Flags().GetString(irDesc)
			if err != nil {
				return err
			}
			*/
			clientCtx := client.GetClientContextFromCmd(cmd)
			conf := config.DefaultConfig()
			dbPath := clientCtx.HomeDir + "/" + conf.DBPath

			cmdr := exec.Command("osmosisd", "status")
			err = cmdr.Run()

			if err == nil {
				// continue only if throws errror
				return nil
			}

			startHeight, err := strconv.ParseInt(irStartHeightFlag, 10, 64)
			if err != nil {
				return err
			}

			endHeight, err := strconv.ParseInt(irEndHeightFlag, 10, 64)
			if err != nil {
				return err
			}

			/*			numThread, err := strconv.ParseInt(irNumThreadFlag, 10, 64)
						if err != nil {
							return err
						}
			*/
			err = indexRangeOfBlocks(dbPath, startHeight, endHeight /*, irConnStrFlag, numThread, irIndexEventFlag == "true", irDescFlag == "true"*/)
			if err != nil {
				return err
			}

			fmt.Println("Done ...")

			return nil
		},
	}

	cmd.Flags().StringP(irStartHeight, "s", "", "Start height to chop to")
	cmd.Flags().StringP(irEndHeight, "e", "", "End height for ABCI to chop to")
	/*cmd.Flags().StringP(irConnStr, "c", "", "psql connection string")
	cmd.Flags().StringP(irNumThread, "n", "", "number of goroutine threads")
	cmd.Flags().StringP(irIndexEvent, "i", "", "boolean to index event")
	cmd.Flags().StringP(irDesc, "d", "", "boolean to desc order")*/
	cmd.MarkFlagRequired(irStartHeight)
	cmd.MarkFlagRequired(irEndHeight)
	/*cmd.MarkFlagRequired(irConnStr)
	cmd.MarkFlagRequired(irNumThread)
	cmd.MarkFlagRequired(irIndexEvent)
	cmd.MarkFlagRequired(irDesc)*/
	return cmd
}

func indexRangeOfBlocks(dbPath string, startHeight int64, endHeight int64 /*, connStr string, numThreads int64, eventIndex bool, desc bool*/) error {
	opts := opt.Options{
		DisableSeeksCompaction: true,
	}

	//fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", "cosmos-indexer-db.cluster-ccko0iyzhafp.us-west-2.rds.amazonaws.com", "manythings", "4aGHhbfVzWCXForGP4EK", "keplrindexerdb")
	//es, err := app.NewEventSink(connStr, "osmosis-1", app.MakeEncodingConfig(), eventIndex)
	/*if err != nil {
		return err
	}*/
	config := app.MakeEncodingConfig()
	txResultFile, err := os.Create("txresult.csv")
	if err != nil {
		return err
	}

	txMsgFile, err := os.Create("txmsg.csv")
	if err != nil {
		return err
	}

	db_bs, err := tmdb.NewGoLevelDBWithOpts("blockstore", dbPath, &opts)
	if err != nil {
		return err
	}

	db_ss, err := tmdb.NewGoLevelDBWithOpts("state", dbPath, &opts)
	if err != nil {
		return err
	}
	jsonpbMarshaller := jsonpb.Marshaler{}

	defer func() {
		db_bs.Close()
		db_ss.Close()
		txResultFile.Close()
		txMsgFile.Close()
		//es.DB().Close()
	}()

	bs := tmstore.NewBlockStore(db_bs)
	ss := tmstate.NewStore(db_ss)

	cnt := int64(0)
	for i := startHeight; i <= endHeight; i++ {
		if cnt%100000 == 0 {
			fmt.Println(startHeight + cnt)
		}
		block := bs.LoadBlock(i)
		abciResponse, err := ss.LoadABCIResponses(i)
		if err != nil {
			return err
		}

		for j := range block.Data.Txs {
			tx := block.Data.Txs[j]
			txr := *abciResponse.DeliverTxs[j]

			cosmosTx, err := config.TxConfig.TxDecoder()(tx)
			if err != nil {
				return err
			}
			codespace, code, info, gasWanted, gasUsed := txr.Codespace, txr.Code, txr.Info, txr.GasWanted, txr.GasUsed
			resultString, err := config.TxConfig.TxJSONEncoder()(cosmosTx)
			txHash := fmt.Sprintf("%X", types.Tx(tx).Hash())

			txResultFile.WriteString(fmt.Sprintf("%d\t%d\t'%s'\t'%s'\t'%s'\t%d\t'%s'\t'%d'\t'%d'\t'%s'\n", block.Height, j, block.Time, txHash, resultString, code, codespace, gasUsed, gasWanted, info))
			//flush to tx_results
			for k, m := range cosmosTx.GetMsgs() {
				for _, s := range m.GetSigners() {
					msgString, err := jsonpbMarshaller.MarshalToString(m)
					if err != nil {
						return err
					}
					msgType := sdk.MsgTypeURL(m)

					txMsgFile.WriteString(fmt.Sprintf("%d\t%d\t%d\t'%s'\t'%s'\t'%s'\n", block.Height, j, k, s, msgString, msgType))
				}
			}
			//flush to tx_msg
		}
		cnt++
	}
	/*window := (endHeight - startHeight) / numThreads

	var wg sync.WaitGroup
	for i := int64(0); i < numThreads; i++ {
		wg.Add(1)
		if desc {
			go func(gbs *tmstore.BlockStore, gss *tmstate.Store, ges *app.EventSink, gfrom int64, gto int64) {
				loadBlockToFrom(gbs, gss, ges, gfrom, gto)
				defer wg.Done()
			}(bs, &ss, es, startHeight+i*window, int64Min(endHeight, startHeight+(i+1)*window))
		} else {
			go func(gbs *tmstore.BlockStore, gss *tmstate.Store, ges *app.EventSink, gfrom int64, gto int64) {
				loadBlockFromTo(gbs, gss, ges, gfrom, gto)
				defer wg.Done()
			}(bs, &ss, es, startHeight+i*window, int64Min(endHeight, startHeight+(i+1)*window))
		}
	}
	//wait!
	wg.Wait()
	*/

	fmt.Println("Done!!")

	return nil
}

func int64Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func loadBlockFromTo(bs *tmstore.BlockStore, ss *tmstate.Store, es *app.EventSink, from int64, to int64) error {
	fmt.Printf("start: %d end: %d\n", from, to)

	cnt := int64(0)
	for i := from; i < to+1; i++ {
		if cnt%100000 == 0 {
			fmt.Println(from + cnt)
		}
		block := bs.LoadBlock(i)
		if block == nil {
			fmt.Println("not able to load block at height %d from the blockstore", i)
			return fmt.Errorf("not able to load block at height %d from the blockstore", i)
		}

		res, err := (*ss).LoadABCIResponses(i)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			fmt.Println("not able to load ABCI Response at height %d from the statestore", i)
			return fmt.Errorf("not able to load ABCI Response at height %d from the statestore", i)
		}

		e := types.EventDataNewBlockHeader{
			Header:           block.Header,
			NumTxs:           int64(len(block.Txs)),
			ResultBeginBlock: *res.BeginBlock,
			ResultEndBlock:   *res.EndBlock,
		}

		err = es.IndexBlockEvents(e)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}

		if e.NumTxs > 0 {
			txrs := []*abcitypes.TxResult{}
			for j := range block.Data.Txs {
				tr := abcitypes.TxResult{
					Height: block.Height,
					Index:  uint32(j),
					Tx:     block.Data.Txs[j],
					Result: *(res.DeliverTxs[j]),
				}
				txrs = append(txrs, &tr)
			}
			es.IndexTxEvents(txrs)
		}
		cnt += 1
	}
	return nil
}

func loadBlockToFrom(bs *tmstore.BlockStore, ss *tmstate.Store, es *app.EventSink, from int64, to int64) error {
	fmt.Printf("start: %d end: %d\n", from, to)

	cnt := int64(0)
	for i := to; i >= from; i-- {
		if cnt%100000 == 0 {
			fmt.Println(to - cnt)
		}
		block := bs.LoadBlock(i)
		if block == nil {
			fmt.Println("not able to load block at height %d from the blockstore", i)
			return fmt.Errorf("not able to load block at height %d from the blockstore", i)
		}

		res, err := (*ss).LoadABCIResponses(i)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			fmt.Println("not able to load ABCI Response at height %d from the statestore", i)
			return fmt.Errorf("not able to load ABCI Response at height %d from the statestore", i)
		}

		e := types.EventDataNewBlockHeader{
			Header:           block.Header,
			NumTxs:           int64(len(block.Txs)),
			ResultBeginBlock: *res.BeginBlock,
			ResultEndBlock:   *res.EndBlock,
		}

		err = es.IndexBlockEvents(e)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}

		if e.NumTxs > 0 {
			txrs := []*abcitypes.TxResult{}
			for j := range block.Data.Txs {
				tr := abcitypes.TxResult{
					Height: block.Height,
					Index:  uint32(j),
					Tx:     block.Data.Txs[j],
					Result: *(res.DeliverTxs[j]),
				}
				txrs = append(txrs, &tr)
			}
			es.IndexTxEvents(txrs)
		}
		cnt += 1
	}
	return nil
}
