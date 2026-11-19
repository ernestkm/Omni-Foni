package main

import (
	"context"
	"errors"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type fakeKafka struct {
	fetches   kgo.Fetches
	polls     int
	cancel    context.CancelFunc
	commits   int
	commitErr error
}

func (f *fakeKafka) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	f.polls++
	if f.polls > 1 {
		if f.cancel != nil {
			f.cancel()
		}
		return nil
	}
	return f.fetches
}

func (f *fakeKafka) CommitRecords(ctx context.Context, _ ...*kgo.Record) error {
	f.commits++
	return f.commitErr
}

type fakeHandler struct {
	calls     int
	failFirst bool
	onHandle  func()
}

func (h *fakeHandler) Handle(ctx context.Context, _ *kgo.Record) error {
	h.calls++
	if h.onHandle != nil {
		h.onHandle()
	}
	if h.failFirst && h.calls == 1 {
		return errors.New("boom")
	}
	return nil
}

func oneRecord(t *testing.T) *kgo.Record {
	t.Helper()
	b, err := proto.Marshal(validEvent(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "report.ready", Partition: 0, Value: b}
}

func TestRunCommitsOnlyAfterHandle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := oneRecord(t)
	f := &fakeKafka{
		fetches: kgo.Fetches{{
			Topics: []kgo.FetchTopic{{
				Topic: "report.ready",
				Partitions: []kgo.FetchPartition{{
					Partition: 0, Records: []*kgo.Record{rec},
				}},
			}},
		}},
		cancel: cancel,
	}
	h := &fakeHandler{}
	h.onHandle = func() {
		if f.commits != 0 {
			t.Fatalf("commit before handle: %d", f.commits)
		}
	}
	Run(ctx, f, h, time.Millisecond)
	if h.calls != 1 || f.commits != 1 {
		t.Fatalf("calls=%d commits=%d", h.calls, f.commits)
	}
}

func TestRunRetriesHandleFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &fakeKafka{
		fetches: kgo.Fetches{{
			Topics: []kgo.FetchTopic{{
				Topic: "report.ready",
				Partitions: []kgo.FetchPartition{{
					Partition: 0, Records: []*kgo.Record{oneRecord(t)},
				}},
			}},
		}},
		cancel: cancel,
	}
	h := &fakeHandler{failFirst: true}
	Run(ctx, f, h, time.Millisecond)
	if h.calls != 2 || f.commits != 1 {
		t.Fatalf("calls=%d commits=%d", h.calls, f.commits)
	}
}
