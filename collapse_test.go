package wavelet

import (
	"github.com/perlin-network/noise/skademlia"
	"github.com/perlin-network/wavelet/avl"
	"github.com/perlin-network/wavelet/store"
	"github.com/perlin-network/wavelet/sys"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"testing"
)

const initialBalance = 100 * 1000 * 1000 * 1000 * 1000

func TestProcessRewardWithdrawals(t *testing.T) {
	state := avl.New(store.NewInmem())

	keys, err := skademlia.NewKeys(1, 1)
	assert.NoError(t, err)

	ctx := NewCollapseContext(state)

	// First reward
	ctx.StoreRewardWithdrawalRequest(RewardWithdrawalRequest{
		account: keys.PublicKey(),
		amount:  1,
		round:   1,
	})

	// Second reward
	ctx.StoreRewardWithdrawalRequest(RewardWithdrawalRequest{
		account: keys.PublicKey(),
		amount:  2,
		round:   2,
	})

	// No reward is withdrew
	ctx.processRewardWithdrawals(50)
	assert.Len(t, ctx.rewardWithdrawalRequests, 2)
	bal, _ := ctx.ReadAccountBalance(keys.PublicKey())
	assert.Equal(t, uint64(0), bal)

	// Withdraw only first reward
	ctx.processRewardWithdrawals(51)
	assert.Len(t, ctx.rewardWithdrawalRequests, 1)
	bal, _ = ctx.ReadAccountBalance(keys.PublicKey())
	assert.Equal(t, uint64(1), bal)

	// Withdraw the second reward
	ctx.processRewardWithdrawals(52)
	assert.Len(t, ctx.rewardWithdrawalRequests, 0)
	bal, _ = ctx.ReadAccountBalance(keys.PublicKey())
	assert.Equal(t, uint64(3), bal)

	assert.NoError(t, ctx.Flush())
	bal, _ = ReadAccountBalance(state, keys.PublicKey())
	assert.Equal(t, uint64(3), bal)
}

func TestCollapseContext(t *testing.T) {
	state := avl.New(store.NewInmem())

	ctx := NewCollapseContext(state)

	// The expected account IDs and its order
	var expectedAccountIDs []AccountID

	checkAccountID := func(override bool, write func(id AccountID)) {
		var id AccountID
		if override {
			// Choose a random account from the slice
			if len(ctx.accountIDs) == 0 {
				assert.FailNow(t, "could not choose a random account because accountIDs slice is empty")
			}
			id = ctx.accountIDs[rand.Intn(len(ctx.accountIDs))]
		} else {
			// Generate a random AccountID that does not exist
			for {
				_, err := rand.Read(id[:])
				assert.NoError(t, err)
				_, exist := ctx.accounts[id]
				if !exist {
					break
				}
			}

			expectedAccountIDs = append(expectedAccountIDs, id)
		}

		write(id)

		_, exist := ctx.accounts[id]
		assert.True(t, exist)
		// Check the account IDs and its ordering
		assert.EqualValues(t, expectedAccountIDs, ctx.accountIDs)
	}

	// For each value, we do a write and check the value.
	f := func(override bool) {

		checkAccountID(override, func(id AccountID) {
			ctx.WriteAccountNonce(id, 1)
			nonce, _ := ctx.ReadAccountNonce(id)
			assert.Equal(t, uint64(1), nonce)
		})

		checkAccountID(override, func(id AccountID) {
			ctx.WriteAccountBalance(id, 2)
			bal, _ := ctx.ReadAccountBalance(id)
			assert.Equal(t, uint64(2), bal)
		})

		checkAccountID(override, func(id AccountID) {
			ctx.WriteAccountStake(id, 3)
			stake, _ := ctx.ReadAccountStake(id)
			assert.Equal(t, uint64(3), stake)
		})

		checkAccountID(override, func(id AccountID) {
			ctx.WriteAccountReward(id, 4)
			reward, _ := ctx.ReadAccountReward(id)
			assert.Equal(t, uint64(4), reward)
		})

		checkAccountID(override, func(id AccountID) {
			ctx.WriteAccountContractGasBalance(id, 5)
			gasBal, _ := ctx.ReadAccountContractGasBalance(id)
			assert.Equal(t, uint64(5), gasBal)
		})

		checkAccountID(override, func(id AccountID) {
			var _code [64]byte
			_, err := rand.Read(_code[:])
			assert.NoError(t, err)

			ctx.WriteAccountContractCode(id, _code[:])
			code, _ := ctx.ReadAccountContractCode(id)
			assert.EqualValues(t, _code[:], code[:])
		})

		checkAccountID(override, func(id AccountID) {
			var mem [64]byte
			_, err := rand.Read(mem[:])
			assert.NoError(t, err)

			var globals = [2]int64{1, 2}

			_vmState := &VMState{
				Globals: globals[:],
				Memory:  mem[:],
			}
			ctx.SetContractState(id, _vmState)
			vmState, _ := ctx.GetContractState(id)
			assert.EqualValues(t, _vmState, vmState)
		})
	}

	// Case 1: new account IDs
	f(false)

	// Case 2: existing account IDs
	f(true)
}

type collapseTestContainer struct {
	graph      *Graph
	accounts   map[AccountID]*skademlia.Keypair
	accountIDs []AccountID

	accountState *Accounts

	end    *Transaction
	viewID uint64
	round  Round
}

func newCollapseContainer(t assert.TestingT, noOfAcc int) *collapseTestContainer {
	if noOfAcc < 2 {
		assert.FailNow(t, "noOfAcc must be at least 2")
	}

	stateStore := store.NewInmem()
	state := avl.New(stateStore)
	var initialRoot Transaction

	viewID := uint64(0)
	state.SetViewID(viewID)

	accounts := make(map[AccountID]*skademlia.Keypair)
	accountIDs := make([]AccountID, 0)
	for i := 0; i < noOfAcc; i++ {
		keys, err := skademlia.NewKeys(1, 1)
		assert.NoError(t, err)

		WriteAccountBalance(state, keys.PublicKey(), initialBalance)

		accounts[keys.PublicKey()] = keys
		accountIDs = append(accountIDs, keys.PublicKey())
	}

	firstAccount := accounts[accountIDs[0]]
	initialRoot = AttachSenderToTransaction(firstAccount, NewTransaction(firstAccount, sys.TagNop, nil))
	graph := NewGraph(WithRoot(initialRoot))

	accountState := NewAccounts(stateStore)
	assert.NoError(t, accountState.Commit(state))

	round := NewRound(viewID, state.Checksum(), 0, Transaction{}, initialRoot)

	testGraph := &collapseTestContainer{
		graph:        graph,
		accounts:     accounts,
		accountIDs:   accountIDs,
		accountState: accountState,
		end:          &initialRoot,
		viewID:       viewID,
		round:        round,
	}

	return testGraph
}

func (g *collapseTestContainer) applyContract(b *testing.B, code []byte) (Transaction, error) {
	rng := rand.New(rand.NewSource(42))

	// Choose a random
	var sender = g.accounts[g.accountIDs[rng.Intn(len(g.accountIDs))]]

	tx := AttachSenderToTransaction(sender,
		NewTransaction(sender, sys.TagContract, buildContractSpawnPayload(100000, 0, code).Marshal()), g.graph.FindEligibleParents()...,
	)

	if err := g.graph.AddTransaction(tx); err != nil {
		return Transaction{}, err
	}

	if _, err := g.collapseTransactions(b); err != nil {
		return Transaction{}, err
	}
	return tx, nil
}

func (g *collapseTestContainer) collapseTransactions(b *testing.B) (*collapseResults, error) {
	if g.end == nil {
		return nil, errors.New("end transaction is nil")
	}
	g.viewID = +1

	results, err := collapseTransactions(g.graph, g.accountState, g.viewID, &g.round, g.round.End, *g.end, false)
	if err != nil {
		return nil, err
	}

	b.StopTimer()

	g.round = NewRound(g.viewID, results.snapshot.Checksum(), uint32(results.appliedCount+results.rejectedCount), g.round.End, *g.end)
	g.end = nil

	b.StartTimer()

	return results, err
}

// Call collapseTransactions with a copy of the account state.
// Use this benchmark collapseTransactions.
func (g *collapseTestContainer) collapseTransactionsNewState(b *testing.B) (*collapseResults, error) {
	if g.end == nil {
		return nil, errors.New("end transaction is nil")
	}

	b.StopTimer()

	accountState := NewAccounts(store.NewInmem())

	if err := accountState.Commit(g.accountState.Snapshot()); err != nil {
		return nil, errors.Wrap(err, "failed to commit")
	}

	b.StartTimer()

	results, err := collapseTransactions(g.graph, accountState, g.viewID+1, &g.round, g.round.End, *g.end, false)
	if err != nil {
		return nil, err
	}

	return results, err
}

func (g *collapseTestContainer) addTxs(b *testing.B, noOfTx int, getTx func(sender *skademlia.Keypair) Transaction) {
	rng := rand.New(rand.NewSource(42))

	var tx Transaction
	for i := 0; i < noOfTx; i++ {
		// Choose random sender
		var sender = g.accounts[g.accountIDs[rng.Intn(len(g.accountIDs))]]

		tx = getTx(sender)

		assert.NoError(b, g.graph.AddTransaction(tx))
	}

	g.end = &tx
}

func (g *collapseTestContainer) addStakeTxs(b *testing.B, noOfTx int) {
	g.addTxs(b, noOfTx, func(sender *skademlia.Keypair) Transaction {
		return AttachSenderToTransaction(sender,
			NewTransaction(sender, sys.TagStake, buildPlaceStakePayload(1).Marshal()),
			g.graph.FindEligibleParents()...,
		)
	})
}

func (g *collapseTestContainer) addTransferTxs(b *testing.B, noOfTx int) {
	rng := rand.New(rand.NewSource(42))

	var recipient *skademlia.Keypair
	g.addTxs(b, noOfTx, func(sender *skademlia.Keypair) Transaction {
		// Choose a random account as the recipient.
		for {
			recipient = g.accounts[g.accountIDs[rng.Intn(len(g.accountIDs))]]
			// Make sure recipient is equal to sender
			if recipient != sender {
				break
			}
		}

		return AttachSenderToTransaction(sender,
			NewTransaction(sender, sys.TagTransfer, buildTransferPayload(recipient.PublicKey(), 1).Marshal()),
			g.graph.FindEligibleParents()...,
		)
	})
}

func (g *collapseTestContainer) addContractTransferTxs(b *testing.B, noOfTx int, sender, contractID AccountID, funcName []byte, amount, gasLimit, gasDeposit uint64) {
	g.addTxs(b, noOfTx, func(sender *skademlia.Keypair) Transaction {
		tx := NewTransaction(
			sender,
			sys.TagTransfer,
			buildTransferWithInvocationPayload(contractID, 200, 500000, []byte("on_money_received"), nil, 0).Marshal(),
		)

		tx = AttachSenderToTransaction(sender,
			tx,
			g.graph.FindEligibleParents()...,
		)

		return tx
	})
}

func (g *collapseTestContainer) addContractCreationTxs(b *testing.B, noOfTx int, code []byte) {
	g.addTxs(b, noOfTx, func(sender *skademlia.Keypair) Transaction {
		return AttachSenderToTransaction(sender,
			NewTransaction(sender, sys.TagContract, buildContractSpawnPayload(100000, 0, code).Marshal()),
			g.graph.FindEligibleParents()...,
		)
	})
}

func BenchmarkCollapseTransactionsStake100(b *testing.B) {
	graph := newCollapseContainer(b, 3)
	graph.addStakeTxs(b, 100)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 100, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsStake1000(b *testing.B) {
	graph := newCollapseContainer(b, 3)
	graph.addStakeTxs(b, 1000)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 1000, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsStake10000(b *testing.B) {
	graph := newCollapseContainer(b, 3)
	graph.addStakeTxs(b, 10000)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 10000, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsStake100000(b *testing.B) {
	graph := newCollapseContainer(b, 3)
	graph.addStakeTxs(b, 100000)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 100000, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsTransfer100(b *testing.B) {
	graph := newCollapseContainer(b, 3)
	graph.addTransferTxs(b, 100)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 100, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsTransfer1000(b *testing.B) {
	graph := newCollapseContainer(b, 3)
	graph.addTransferTxs(b, 1000)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 1000, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsTransfer10000(b *testing.B) {
	graph := newCollapseContainer(b, 3)
	graph.addTransferTxs(b, 10000)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 10000, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsTransfer100000(b *testing.B) {
	graph := newCollapseContainer(b, 3)
	graph.addTransferTxs(b, 100000)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 100000, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsContractCreation100(b *testing.B) {
	code, err := ioutil.ReadFile("testdata/transfer_back.wasm")
	assert.NoError(b, err)

	graph := newCollapseContainer(b, 3)
	graph.addContractCreationTxs(b, 100, code)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 100, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsContractCreation1000(b *testing.B) {
	code, err := ioutil.ReadFile("testdata/transfer_back.wasm")
	assert.NoError(b, err)

	graph := newCollapseContainer(b, 3)
	graph.addContractCreationTxs(b, 1000, code)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactions(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 1000, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsContractCreation10000(b *testing.B) {
	code, err := ioutil.ReadFile("testdata/transfer_back.wasm")
	assert.NoError(b, err)

	graph := newCollapseContainer(b, 3)
	graph.addContractCreationTxs(b, 10000, code)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 10000, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsContractTransfer100(b *testing.B) {
	code, err := ioutil.ReadFile("testdata/transfer_back.wasm")
	assert.NoError(b, err)

	graph := newCollapseContainer(b, 3)

	contract, err := graph.applyContract(b, code)
	if err != nil {
		b.Fatal(err)
	}

	graph.addContractTransferTxs(b, 100, contract.Sender, contract.ID, []byte("on_money_received"), 200, 500000, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 101, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsContractTransfer1000(b *testing.B) {
	code, err := ioutil.ReadFile("testdata/transfer_back.wasm")
	assert.NoError(b, err)

	graph := newCollapseContainer(b, 3)

	contract, err := graph.applyContract(b, code)
	if err != nil {
		b.Fatal(err)
	}

	graph.addContractTransferTxs(b, 1000, contract.Sender, contract.ID, []byte("on_money_received"), 200, 500000, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 1001, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsContractTransfer10000(b *testing.B) {
	code, err := ioutil.ReadFile("testdata/transfer_back.wasm")
	assert.NoError(b, err)

	graph := newCollapseContainer(b, 3)

	contract, err := graph.applyContract(b, code)
	if err != nil {
		b.Fatal(err)
	}

	graph.addContractTransferTxs(b, 10000, contract.Sender, contract.ID, []byte("on_money_received"), 200, 500000, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 10001, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsContractTransfer100000(b *testing.B) {
	code, err := ioutil.ReadFile("testdata/transfer_back.wasm")
	assert.NoError(b, err)

	graph := newCollapseContainer(b, 3)

	contract, err := graph.applyContract(b, code)
	if err != nil {
		b.Fatal(err)
	}

	graph.addContractTransferTxs(b, 100000, contract.Sender, contract.ID, []byte("on_money_received"), 200, 500000, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 100001, results.appliedCount)
	}
}

func BenchmarkCollapseTransactionsMixed(b *testing.B) {
	code, err := ioutil.ReadFile("testdata/transfer_back.wasm")
	assert.NoError(b, err)

	graph := newCollapseContainer(b, 3)

	contract, err := graph.applyContract(b, code)
	if err != nil {
		b.Fatal(err)
	}

	graph.addContractTransferTxs(b, 30000, contract.Sender, contract.ID, []byte("on_money_received"), 200, 500000, 0)
	graph.addTransferTxs(b, 30000)
	graph.addStakeTxs(b, 30000)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := graph.collapseTransactionsNewState(b)
		if err != nil {
			b.Fatal(err)
		}
		assert.Equal(b, 1+30000+30000+30000, results.appliedCount)
	}
}

//func TestCollapseTransactions(t *testing.T) {
//	stateStore := store.NewInmem()
//	state := avl.New(stateStore)
//
//	viewID := uint64(0)
//	state.SetViewID(viewID)
//
//	accounts := make(map[AccountID]*skademlia.Keypair)
//	accountIDs := make([]AccountID, 0)
//	for i := 0; i < noOfAcc; i++ {
//		keys, err := skademlia.NewKeys(1, 1)
//		assert.NoError(t, err)
//
//		WriteAccountBalance(state, keys.PublicKey(), initialBalance)
//
//		accounts[keys.PublicKey()] = keys
//		accountIDs = append(accountIDs, keys.PublicKey())
//	}
//
//	firstAccount := accounts[accountIDs[0]]
//	initialRoot := AttachSenderToTransaction(firstAccount, NewTransaction(firstAccount, sys.TagNop, nil))
//	graph := NewGraph(WithRoot(initialRoot))
//
//	accountState := NewAccounts(stateStore)
//	assert.NoError(t, accountState.Commit(state))
//
//	round := NewRound(viewID, state.Checksum(), 0, Transaction{}, initialRoot)
//
//}
