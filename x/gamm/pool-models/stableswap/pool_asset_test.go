package stableswap_test

import (
	"fmt"
	"testing"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/require"

	"github.com/osmosis-labs/osmosis/v7/x/gamm/pool-models/stableswap"
	types "github.com/osmosis-labs/osmosis/v7/x/gamm/types"
)

func TestStableSwapPoolAssetValidate(t *testing.T) {
	testcase := map[string]struct {
		name          string
		tokenAmount   int64
		scalingFactor int64
		expected      error
	}{
		"valid pool asset": {
			name:          ustDenom,
			tokenAmount:   100,
			scalingFactor: 10,
		},
		"empty denom": {
			name:          "",
			tokenAmount:   100,
			scalingFactor: 10,
			expected:      types.ErrEmptyPoolAssets,
		},
		"zero scaling factor - invalid": {
			name:          ustDenom,
			tokenAmount:   100,
			scalingFactor: 0,
			expected:      fmt.Errorf(stableswap.ErrMsgFmtNonPositiveScalingFactor, ustDenom, 0),
		},
		"zero token amount - invalid": {
			name:          ustDenom,
			tokenAmount:   0,
			scalingFactor: 10,
			expected:      fmt.Errorf(stableswap.ErrMsgFmtNonPositiveTokenAmount, ustDenom, 0),
		},
	}
	for name, tc := range testcase {
		t.Run(name, func(t *testing.T) {
			poolAsset := stableswap.PoolAsset{
				Token:         sdk.Coin{tc.name, sdk.NewInt(tc.tokenAmount)},
				ScalingFactor: sdk.NewInt(tc.scalingFactor),
			}

			res := poolAsset.Validate()

			require.Equal(t, tc.expected, res)
		})
	}
}
