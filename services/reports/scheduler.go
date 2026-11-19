package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel"

	"omnifoni/observability"
	schedulingpb "omnifoni/reports/gen/schedulingpb"
)

// ReportPublisher announces stored artifacts; the scheduler uses it for
// notifications only, never for storage.
type ReportPublisher interface {
	PublishReportReady(ctx context.Context, ev *schedulingpb.ReportReadyEvent) error
}

type kafkaReportPublisher struct {
	prod  *kgo.Client
	topic string
}

func newKafkaReportPublisher(prod *kgo.Client, topic string) ReportPublisher {
	return &kafkaReportPublisher{prod: prod, topic: topic}
}

func (p *kafkaReportPublisher) PublishReportReady(ctx context.Context, ev *schedulingpb.ReportReadyEvent) error {
	b, err := proto.Marshal(ev)
	if err != nil {
		return err
	}
	r := &kgo.Record{Topic: p.topic, Key: []byte(ev.GetScheduleId()), Value: b}
	observability.InjectKafka(ctx, r)
	return p.prod.ProduceSync(ctx, r).FirstErr()
}

// Scheduler renders due schedules into CSV artifacts.
type Scheduler struct {
	store  *SchedStore
	api    API
	pub    ReportPublisher
	signer *ArtifactSigner
}

func NewScheduler(store *SchedStore, api API, pub ReportPublisher, signer *ArtifactSigner) *Scheduler {
	return &Scheduler{store: store, api: api, pub: pub, signer: signer}
}

// due reports whether the schedule fires at the given UTC wall-clock time.
func due(s Schedule, now time.Time) bool {
	if !s.Enabled {
		return false
	}
	u := now.UTC()
	if u.Hour() != s.AtHour || u.Minute() != s.AtMinute {
		return false
	}
	switch s.Frequency {
	case "daily":
		return true
	case "weekly":
		return s.Weekday != nil && int(u.Weekday()) == *s.Weekday
	case "monthly":
		if s.DayOfMonth == nil {
			return false
		}
		last := time.Date(u.Year(), u.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
		dom := *s.DayOfMonth
		if dom > last {
			dom = last
		}
		return u.Day() == dom
	}
	return false
}

// resolvePeriod maps a window kind to concrete UTC bounds.
func resolvePeriod(kind string, now time.Time) (time.Time, time.Time, error) {
	u := now.UTC()
	switch kind {
	case "last_day":
		return u.Add(-24 * time.Hour), u, nil
	case "last_week":
		return u.Add(-7 * 24 * time.Hour), u, nil
	case "last_month":
		return u.Add(-30 * 24 * time.Hour), u, nil
	case "previous_month":
		start := time.Date(u.Year(), u.Month()-1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(u.Year(), u.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start, end, nil
	}
	return time.Time{}, time.Time{}, fmt.Errorf("unknown window_kind %q", kind)
}

// RunDueSchedules renders every enabled schedule whose slot matches now.
// Per-schedule failures are logged and never block the other schedules.
func (s *Scheduler) RunDueSchedules(ctx context.Context, now time.Time) error {
	scheds, err := s.store.ListEnabledSchedules(ctx)
	if err != nil {
		return err
	}
	slot := now.UTC().Format("2006-01-02T15:04Z")
	var firstErr error
	for _, sch := range scheds {
		if !due(sch, now) {
			continue
		}
		from, to, err := resolvePeriod(sch.WindowKind, now)
		if err != nil {
			slog.ErrorContext(ctx, "scheduled report bad window", "schedule", sch.ScheduleID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err := s.runOne(ctx, sch, slot, from, to); err != nil {
			slog.ErrorContext(ctx, "scheduled report failed", "schedule", sch.ScheduleID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// RunScheduleNow renders immediately with a unique manual slot key.
func (s *Scheduler) RunScheduleNow(ctx context.Context, sch Schedule) (Artifact, error) {
	from, to, err := resolvePeriod(sch.WindowKind, time.Now())
	if err != nil {
		return Artifact{}, err
	}
	return s.runOne(ctx, sch, "manual-"+uuid.NewString(), from, to)
}

func (s *Scheduler) runOne(ctx context.Context, sch Schedule, slot string, from, to time.Time) (Artifact, error) {
	ctx, span := otel.Tracer("reports").Start(ctx, "reports.scheduled_run")
	defer span.End()

	body, rows, err := renderReportCSV(ctx, s.api, sch, from, to)
	if err != nil {
		return Artifact{}, err
	}
	a, inserted, err := s.store.InsertArtifact(ctx, Artifact{
		ScheduleID: sch.ScheduleID, SlotKey: slot, PeriodFrom: from, PeriodTo: to,
		RowCount: rows, CSVBody: body,
	})
	if err != nil || !inserted {
		return Artifact{}, err
	}
	if s.pub != nil {
		ev := &schedulingpb.ReportReadyEvent{
			ScheduleId: a.ScheduleID, ArtifactId: a.ArtifactID, TenantId: sch.TenantID,
			ReportKind: sch.ReportKind,
			PeriodFrom: a.PeriodFrom.UTC().Format(time.RFC3339),
			PeriodTo:   a.PeriodTo.UTC().Format(time.RFC3339),
			RowCount:   a.RowCount, CreatedAtMs: a.CreatedAt.UnixMilli(),
			RecipientEmail: sch.RecipientEmail,
		}
		if s.signer != nil {
			ev.ArtifactUrl, ev.ExpiresAtMs = s.signer.Sign(a.ScheduleID, a.ArtifactID, time.Now())
		}
		if err := s.pub.PublishReportReady(ctx, ev); err != nil {
			slog.ErrorContext(ctx, "report.ready publish", "schedule", sch.ScheduleID, "err", err)
		}
	}
	return a, nil
}
