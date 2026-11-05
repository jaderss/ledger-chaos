package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Kafka struct {
	brokers []string
	admin   *kadm.Client
	client  *kgo.Client
}

func NewKafka(brokers []string) (*Kafka, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	return &Kafka{brokers: brokers, admin: kadm.NewClient(client), client: client}, nil
}

func (k *Kafka) Close() {
	k.client.Close()
}

func (k *Kafka) offsets(ctx context.Context, topic string) (start, end map[int32]int64, err error) {
	starts, err := k.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, nil, err
	}
	ends, err := k.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, nil, err
	}
	start, end = map[int32]int64{}, map[int32]int64{}
	var listErr error
	starts.Each(func(o kadm.ListedOffset) {
		listErr = firstErr(listErr, o.Err)
		start[o.Partition] = o.Offset
	})
	ends.Each(func(o kadm.ListedOffset) {
		listErr = firstErr(listErr, o.Err)
		end[o.Partition] = o.Offset
	})
	return start, end, listErr
}

func firstErr(current, next error) error {
	if current != nil {
		return current
	}
	return next
}

func (k *Kafka) Count(ctx context.Context, topic string) (int64, error) {
	start, end, err := k.offsets(ctx, topic)
	var n int64
	for p, e := range end {
		n += e - start[p]
	}
	return n, err
}

func (k *Kafka) Lag(ctx context.Context, group, topic string) (int64, error) {
	start, end, err := k.offsets(ctx, topic)
	if err != nil {
		return 0, err
	}
	committed, err := k.admin.FetchOffsets(ctx, group)
	if err != nil {
		return 0, err
	}
	var lag int64
	for p, e := range end {
		at := start[p]
		if o, ok := committed.Lookup(topic, p); ok && o.Err == nil && o.At >= 0 {
			at = o.At
		}
		lag += e - at
	}
	return lag, nil
}

func (k *Kafka) Events(ctx context.Context, topic string) ([]Event, error) {
	start, end, err := k.offsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	remaining := map[int32]int64{}
	for p, e := range end {
		if e > start[p] {
			remaining[p] = e
		}
	}
	if len(remaining) == 0 {
		return nil, nil
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(k.brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return nil, err
	}
	defer consumer.Close()
	var events []Event
	for len(remaining) > 0 {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("reading %s: %w", topic, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			events = append(events, parseEvent(r.Value, fmt.Sprintf("%d/%d", r.Partition, r.Offset)))
			if r.Offset+1 >= remaining[r.Partition] {
				delete(remaining, r.Partition)
			}
		})
	}
	return events, nil
}

func parseEvent(value []byte, position string) Event {
	var raw struct {
		EventID    string `json:"eventId"`
		TransferID string `json:"transferId"`
		Postings   []struct {
			AccountID   string `json:"accountId"`
			AmountMinor int64  `json:"amountMinor"`
		} `json:"postings"`
	}
	if err := json.Unmarshal(value, &raw); err != nil {
		return Event{EventID: "unparseable at " + position}
	}
	e := Event{EventID: raw.EventID, TransferID: raw.TransferID, Postings: map[string]int64{}}
	for _, p := range raw.Postings {
		e.Postings[p.AccountID] += p.AmountMinor
	}
	return e
}
