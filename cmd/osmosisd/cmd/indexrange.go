package cmd

// DONTCOVER

import (
	"fmt"
	app "github.com/osmosis-labs/osmosis/v10/app"
	abcitypes "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/types"
	"os/exec"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/syndtr/goleveldb/leveldb/opt"
	tmstate "github.com/tendermint/tendermint/state"
	tmstore "github.com/tendermint/tendermint/store"
	tmdb "github.com/tendermint/tm-db"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/tendermint/tendermint/config"
)

const (
	irStartHeight = "irStartHeight"
	irEndHeight   = "irEndHeight"
)

// get cmd to convert any bech32 address to an osmo prefix.
func indexRange() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "indexrange",
		Short: "Example osmosisd indexrange -s 1 -e 1000, which would index block data from s height to e height.",
		Long:  "Index range options indexes blocks at given range from blockstore.db. One needs to shut down chain before running indexrange.",
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

			err = indexRangeOfBlocks(dbPath, startHeight, endHeight)
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

func indexRangeOfBlocks(dbPath string, startHeight int64, endHeight int64) error {
	opts := opt.Options{
		DisableSeeksCompaction: true,
	}

	es, err := app.NewEventSink("postgresql://postgres:vmfXaJfF7o1BEmxxYaOJ@cosmos-indexer-db.ccko0iyzhafp.us-west-2.rds.amazonaws.com:5432", "osmosis-1", app.MakeEncodingConfig())
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

	defer func() {
		db_bs.Close()
		db_ss.Close()
		es.DB().Close()
	}()

	bs := tmstore.NewBlockStore(db_bs)
	ss := tmstate.NewStore(db_ss)

	count := 0
	for i := startHeight; i < endHeight+1; i++ {
		if count/100000 == 0 {
			fmt.Println(count)
		}
		block := bs.LoadBlock(i)
		if block == nil {
			return fmt.Errorf("not able to load block at height %d from the blockstore", i)
		}
		res, err := ss.LoadABCIResponses(i)
		if err != nil {
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
		count += 1
	}

	fmt.Println("Done!!")

	return nil
}
