package main

import (
	"context"
	"testing"
)

type fakeKam struct {
	ended [][2]string
	err   error
}

func (f *fakeKam) endDialog(_ context.Context, hEntry, hID string) error {
	f.ended = append(f.ended, [2]string{hEntry, hID})
	return f.err
}

func TestReaperTickEndsGoneCalls(t *testing.T) {
	st := newStore()
	st.put("ci-1", entry{switchingID: "sw-1", hEntry: "7", hID: "9"})
	st.put("ci-2", entry{switchingID: "sw-2", hEntry: "8", hID: "10"})

	r := &reaper{store: st, kam: &fakeKam{}, live: func(ctx context.Context) (map[string]bool, error) {
		return map[string]bool{"sw-2": true}, nil
	}}
	if err := r.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(r.kam.(*fakeKam).ended) != 1 || r.kam.(*fakeKam).ended[0] != [2]string{"7", "9"} {
		t.Fatalf("unexpected ends: %+v", r.kam.(*fakeKam).ended)
	}
	if _, ok := st.get("ci-1"); ok {
		t.Fatal("gone call still tracked")
	}
	if _, ok := st.get("ci-2"); !ok {
		t.Fatal("live call was untracked")
	}
}
