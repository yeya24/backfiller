package main

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/tsdb"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	prom_rules "github.com/prometheus/prometheus/rules"
	store_tsdb "github.com/prometheus/prometheus/storage/tsdb"
)

var (
	defaultDBPath = "data"
)

// Default duration of a block in milliseconds - 2h.
const (
	defaultBlockDuration = int64(2 * 60 * 60 * 1000)
)

func main() {
	app := kingpin.New(filepath.Base(os.Args[0]), "Tooling for backfilling Prometheus Recording Rules.")
	app.Version("v0.0.1")
	app.HelpFlag.Short('h')

	ruleFile := app.Arg(
		"rule-file",
		"The rule file to do backfilling.",
	).Required().ExistingFile()

	dbPath := app.Arg("db path", "database path (default is " + defaultDBPath + ")").Default(defaultDBPath).String()
	destPath := app.Arg("dest path", "path to generate new block").Default("dest").String()

	kingpin.MustParse(app.Parse(os.Args[1:]))
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
	opts := &tsdb.Options{
		BlockRanges: []int64{defaultBlockDuration},
		WALSegmentSize: 0,
		NoLockfile:     true,
	}

	db, err := tsdb.Open(*dbPath, logger, prometheus.DefaultRegisterer, opts)
	if err != nil {
		logger.Log("err", err)
		os.Exit(1)
	}
	defer db.Close()

	os.Exit(backfillRule(db, *destPath, *ruleFile, logger))
}

type recordingRule struct {
	name     string
	interval time.Duration
	vector   promql.Expr
	lset     labels.Labels
}

func NewRecordingRule(name string, interval model.Duration, vector promql.Expr, lset labels.Labels) *recordingRule {
	return &recordingRule{name, time.Duration(interval), vector, lset}
}

func backfillRule(db *tsdb.DB, dest, filename string, logger log.Logger) int {
	rgs, errs := rulefmt.ParseFile(filename)
	if errs != nil {
		logger.Log("err", errs)
		return 1
	}

	var rules []*recordingRule
	for _, rg := range rgs.Groups {
		for _, rule := range rg.Rules {
			if rule.Record != "" {
				expr, err := promql.ParseExpr(rule.Expr)
				if err != nil {
					logger.Log("err", err)
					return 1
				}
				rules = append(rules,
					NewRecordingRule(rule.Record, rg.Interval, expr, labels.FromMap(rule.Labels)))
			}
		}
	}

	opts := promql.EngineOpts{
		Logger:        logger,
		Reg:           prometheus.DefaultRegisterer,
		MaxConcurrent: 20,
		MaxSamples:    50000000,
		Timeout:       10 * time.Second,
	}

	queryEngine := promql.NewEngine(opts)

	// Set the maxtime to now - 1s
	minTime, maxTime := db.Head().MinTime(), (time.Now().Unix() - 1) * 1000
	for _, block := range db.Blocks() {
		minTime = min(minTime, block.MinTime())
	}

	var localStorage = &store_tsdb.ReadyStorage{}
	startTimeMargin := int64(2 * 2 * time.Hour.Seconds() * 1000)
	localStorage.Set(db, startTimeMargin)

	mss := []*tsdb.MetricSample{}
	queryFunc := prom_rules.EngineQueryFunc(queryEngine, localStorage)
	for _, rule := range rules {
		for t := maxTime; t >= minTime + rule.interval.Milliseconds(); t -= rule.interval.Milliseconds() {
			vector, err := queryFunc(context.Background(), rule.vector.String(), time.Unix(t/1e3, 0))
			if err != nil {
				logger.Log("err", err)
				return 1
			}
			for _, sample := range vector {
				lb := labels.NewBuilder(sample.Metric)
				lb.Set(labels.MetricName, rule.name)

				for _, l := range rule.lset {
					lb.Set(l.Name, l.Value)
				}
				mss = append(mss, &tsdb.MetricSample{Labels: lb.Labels(), Value: sample.V, TimestampMs: sample.T})
			}
		}
	}

	blockID, err :=  tsdb.CreateBlock(mss, dest, minTime, maxTime, logger)
	if err != nil {
		logger.Log("err", err)
		return 1
	}

	logger.Log("blockId", blockID)

	return 0
}

func min(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}
