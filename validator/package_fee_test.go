package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	arcerrors "github.com/bsv-blockchain/arcade/errors"
)

// packageFeeFixture returns a linked pair whose parent pays one satoshi and
// whose child pays enough to cover the 100 sat/kB floor for the pair. The
// source outputs are attached so validation exercises scripts and fees.
func packageFeeFixture(t *testing.T) (parent, child *sdkTx.Transaction, parentRaw, childRaw []byte) {
	parent = sdkTx.NewTransaction()
	parent.Version = 2
	require.NoError(t, parent.AddInputFrom(
		"0000000000000000000000000000000000000000000000000000000000000001",
		0, "51", 100_000, nil,
	))
	parent.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      99_999,
		LockingScript: script.NewFromBytes([]byte{script.OpTRUE}),
	})
	parent.Inputs[0].UnlockingScript = script.NewFromBytes([]byte{script.OpTRUE})

	child = sdkTx.NewTransaction()
	child.Version = 2
	child.AddInputFromTx(parent, 0, nil)
	child.Inputs[0].UnlockingScript = script.NewFromBytes([]byte{script.OpTRUE})
	child.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      98_999,
		LockingScript: script.NewFromBytes([]byte{script.OpTRUE}),
	})

	var err error
	parentRaw, err = parent.EF()
	require.NoError(t, err)
	childRaw, err = child.EF()
	require.NoError(t, err)
	return
}

func TestPackageFeeAncestorBelowFloorIsRejectedIndividually(t *testing.T) {
	parent, child, parentRaw, childRaw := packageFeeFixture(t)
	minFeePerKB := uint64(100)
	parentFee, err := parent.GetFee()
	require.NoError(t, err)
	childFee, err := child.GetFee()
	require.NoError(t, err)
	assert.NotEmpty(t, parentRaw)
	assert.NotEmpty(t, childRaw)

	parentFloor := (uint64(len(parent.Bytes()))*minFeePerKB + 999) / 1000
	packageFloor := (uint64(len(parent.Bytes())+len(child.Bytes()))*minFeePerKB + 999) / 1000
	assert.Equal(t, uint64(1), parentFee)
	assert.Less(t, parentFee, parentFloor)
	assert.GreaterOrEqual(t, childFee, packageFloor)
	assert.GreaterOrEqual(t, parentFee+childFee, packageFloor)

	v := NewValidator(&Policy{MinFeePerKB: &minFeePerKB})
	ctx := context.Background()
	require.NoError(t, v.ValidateTransaction(ctx, parent, true), "parent should pass non-fee script/structure checks")
	require.NoError(t, v.ValidateTransaction(ctx, child, false), "child should pass its own fee check")

	err = v.ValidateTransaction(ctx, parent, false)
	require.Error(t, err)
	arcErr := arcerrors.GetArcError(err)
	require.NotNil(t, arcErr)
	assert.Equal(t, arcerrors.StatusFees, arcErr.StatusCode)
}
