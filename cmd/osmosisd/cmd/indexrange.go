package cmd

// DONTCOVER

import (
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	app "github.com/osmosis-labs/osmosis/v10/app"
	"github.com/spf13/cobra"
	"github.com/syndtr/goleveldb/leveldb/opt"
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
	cmd.MarkFlagRequired(irStartHeight)
	cmd.MarkFlagRequired(irEndHeight)
	return cmd
}

func indexRangeOfBlocks(dbPath string, startHeight int64, endHeight int64 /*, connStr string, numThreads int64, eventIndex bool, desc bool*/) error {
	opts := opt.Options{
		DisableSeeksCompaction: true,
	}

	config := app.MakeEncodingConfig()
	txResultFile, err := os.OpenFile("txresult.csv", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}

	txMsgFile, err := os.OpenFile("txmsg.csv", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
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
				fmt.Printf("decode error at height %d\n", block.Height)
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

	fmt.Println("Done!!")

	return nil
}
