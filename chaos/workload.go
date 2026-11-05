package main

import (
	"context"
	"crypto/rand"
	"fmt"
	mrand "math/rand/v2"
	"sync"
	"time"
)

type Workload struct {
	ledger        *LedgerClient
	log           *Log
	currency      string
	mint          string
	accounts      []string
	workers       int
	retries       int
	clientTimeout time.Duration
	amountMax     int64
	duplicateRate float64
	mismatchRate  float64
	seed          uint64

	mu      sync.Mutex
	settled []settled
}

type settled struct {
	key  string
	body Body
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (w *Workload) Setup(ctx context.Context, accounts int, overdraftEvery int) error {
	w.mint = newID()
	if err := w.ledger.CreateAccount(ctx, w.mint, w.currency, true); err != nil {
		return err
	}
	for i := range accounts {
		id := newID()
		if err := w.ledger.CreateAccount(ctx, id, w.currency, overdraftEvery > 0 && i%overdraftEvery == 0); err != nil {
			return err
		}
		w.accounts = append(w.accounts, id)
	}
	return nil
}

func (w *Workload) Fund(ctx context.Context, amount int64) error {
	for _, account := range w.accounts {
		body := Body{From: w.mint, To: account, Amount: amount, Currency: w.currency}
		key := newID()
		for {
			if r := w.attempt(-1, key, body, false); r.Phase == PhaseOK {
				break
			} else if r.Phase == PhaseFail {
				return fmt.Errorf("funding %s refused: %d %s", account, r.Status, r.Problem)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return nil
}

func (w *Workload) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for id := range w.workers {
		wg.Go(func() { w.worker(ctx, id) })
	}
	wg.Wait()
}

func (w *Workload) worker(ctx context.Context, id int) {
	rng := mrand.New(mrand.NewPCG(w.seed, uint64(id)))
	for ctx.Err() == nil {
		roll := rng.Float64()
		switch {
		case roll < w.mismatchRate:
			if s, ok := w.pickSettled(rng); ok {
				other := s.body
				other.Amount++
				w.attempt(id, s.key, other, true)
				continue
			}
			w.send(id, newID(), w.randomBody(rng))
		case roll < w.mismatchRate+w.duplicateRate:
			key, body := newID(), w.randomBody(rng)
			var wg sync.WaitGroup
			wg.Go(func() { w.send(id, key, body) })
			wg.Go(func() { w.send(id, key, body) })
			wg.Wait()
		default:
			w.send(id, newID(), w.randomBody(rng))
		}
	}
}

func (w *Workload) randomBody(rng *mrand.Rand) Body {
	from := rng.IntN(len(w.accounts))
	to := rng.IntN(len(w.accounts) - 1)
	if to >= from {
		to++
	}
	return Body{From: w.accounts[from], To: w.accounts[to], Amount: 1 + rng.Int64N(w.amountMax), Currency: w.currency}
}

func (w *Workload) send(worker int, key string, body Body) {
	for range w.retries + 1 {
		r := w.attempt(worker, key, body, false)
		if r.Phase != PhaseUnknown {
			if r.Phase == PhaseOK {
				w.mu.Lock()
				w.settled = append(w.settled, settled{key: key, body: body})
				w.mu.Unlock()
			}
			return
		}
	}
}

func (w *Workload) attempt(worker int, key string, body Body, mismatch bool) Result {
	w.log.Write(Entry{T: now(), Worker: worker, Op: "transfer", Key: key, Phase: "invoke", Body: &body, Mismatch: mismatch})
	ctx, cancel := context.WithTimeout(context.Background(), w.clientTimeout)
	defer cancel()
	r := w.ledger.Transfer(ctx, key, body)
	w.log.Write(Entry{
		T: now(), Worker: worker, Op: "transfer", Key: key, Phase: r.Phase, Mismatch: mismatch,
		Status: r.Status, TransferID: r.TransferID, TransferStatus: r.TransferStatus, Problem: r.Problem,
		Replayed: r.Replayed, Error: r.Error,
	})
	if r.Phase == PhaseUnknown && r.Error != "" {
		time.Sleep(50 * time.Millisecond)
	}
	return r
}

func (w *Workload) pickSettled(rng *mrand.Rand) (settled, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.settled) == 0 {
		return settled{}, false
	}
	return w.settled[rng.IntN(len(w.settled))], true
}
