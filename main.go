package main

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/logging"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	prom_rules "github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/wal"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	defaultDBPath        = "data/"
)

type recordingRule struct {
	name   string
	vector parser.Expr
	lset   labels.Labels
}

func main() {
	app := kingpin.New(filepath.Base(os.Args[0]), "Tooling for backfilling Prometheus Recording Rules.")
	app.Version("v0.0.1")
	app.HelpFlag.Short('h')

	ruleFile := app.Arg("rule-file", "The rule file for backfilling.").Required().ExistingFile()

	dbPath := app.Arg("db path", "tsdb path (default is "+defaultDBPath+")").Default(defaultDBPath).String()

	destPath := app.Arg("dest path", "path to generate new block (default is "+defaultDBPath+")").Default(defaultDBPath).String()

	maxSamples := app.Flag("max-samples", "Maximum number of samples a single query can load into memory. Note that queries will fail if they try to load more samples than this into memory, so this also limits the number of samples a query can return.").
		Default("50000000").Int()

	timeout := app.Flag("timeout", "Maximum time a query may take before being aborted.").
		Default("2m").Duration()

	start := app.Flag("start", "Start time (RFC3339 or Unix timestamp).").String()
	end := app.Flag("end", "End time (RFC3339 or Unix timestamp).").String()

	evalInterval := app.Flag("eval-interval", "How frequently to evaluate the recording rules.").Default("30s").Duration()
	maxSamplesInMem := app.Flag("max-samples-in-mem", "maximum number of samples to process in a cycle.").Default("10000").Int()
	queryLogFile := app.Flag("query-log-file", "File to which PromQL queries are logged.").Default("").String()

	logCfg := &promlog.Config{}
	flag.AddFlags(app, logCfg)

	kingpin.MustParse(app.Parse(os.Args[1:]))
	logger := promlog.New(logCfg)

	rules, errs := parseRules(*ruleFile, logger)
	if errs != nil {
		for _, e := range errs {
			level.Error(logger).Log("msg", "loading groups failed", "err", e)
		}
		return
	}

	opts := &tsdb.Options{
		WALSegmentSize: wal.DefaultSegmentSize,
		NoLockfile:     true,
	}

	db, err := tsdb.Open(*dbPath, logger, prometheus.DefaultRegisterer, opts)
	if err != nil {
		level.Error(logger).Log("msg", "failed to open TSDB", "path", *dbPath, "err", err)
		return
	}
	defer db.Close()

	tr, err := getTimeRange(db, *start, *end)
	if err != nil {
		level.Error(logger).Log("err", err)
		return
	}

	queryEngine := newQueryEngine(*maxSamples, *timeout, logger)
	if *queryLogFile == "" {
		queryEngine.SetQueryLogger(nil)
	} else {
		l, err := logging.NewJSONFileLogger(*queryLogFile)
		if err != nil {
			level.Error(logger).Log("msg", "failed to create query logger", "err", err)
		}
		queryEngine.SetQueryLogger(l)
	}

	queryFunc := prom_rules.EngineQueryFunc(queryEngine, db)
	backfillRules(rules, *destPath, tr, evalInterval.Milliseconds(), *maxSamplesInMem, queryFunc, logger)

	return
}

func newQueryEngine(maxSamples int, timeout time.Duration, logger log.Logger) *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		Logger:     logger,
		Reg:        prometheus.DefaultRegisterer,
		MaxSamples: maxSamples,
		Timeout:    timeout,
	})
}

func parseRules(filename string, logger log.Logger) ([]*recordingRule, []error) {
	rgs, errs := rulefmt.ParseFile(filename)
	if errs != nil {
		return nil, errs
	}

	var rules []*recordingRule
	for _, rg := range rgs.Groups {
		for _, rule := range rg.Rules {
			// We only consider recording rules.
			if rule.Record.Value != "" {
				expr, err := parser.ParseExpr(rule.Expr.Value)
				if err != nil {
					level.Error(logger).Log("msg", "failed to parse expr", "expr", rule.Expr, "err", err)
					return nil, []error{errors.Wrap(err, filename)}
				}
				rules = append(rules, &recordingRule{rule.Record.Value, expr, labels.FromMap(rule.Labels)})
			}
		}
	}

	return rules, nil
}

type timeRange struct {
	start time.Time
	end   time.Time
}

func getTimeRange(db *tsdb.DB, start, end string) (*timeRange, error) {
	var (
		stime, etime time.Time
		err          error
	)

	minTime, maxTime := db.Head().MinTime(), db.Head().MaxTime()
	for _, block := range db.Blocks() {
		minTime = min(minTime, block.MinTime())
	}

	if start != "" {
		stime, err = parseTime(start)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse start time")
		}
		if timestamp.FromTime(stime) < minTime {
			stime = timestamp.Time(minTime)
		}
	} else {
		stime = timestamp.Time(minTime)
	}

	if end != "" {
		etime, err = parseTime(end)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse end time")
		}
		if timestamp.FromTime(etime) > maxTime {
			etime = timestamp.Time(maxTime)
		}
	} else {
		etime = timestamp.Time(maxTime)
	}

	if stime.After(etime) {
		return nil, errors.New("start time should be before end time")
	}

	return &timeRange{stime, etime}, nil
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}

func backfillRules(rules []*recordingRule, dest string, tr *timeRange, evalInterval int64, maxSamples int, queryFunc prom_rules.QueryFunc, logger log.Logger) {
	start := timestamp.FromTime(tr.start)
	end := timestamp.FromTime(tr.end)

	var mss []*tsdb.MetricSample
	var minTime int64 = math.MaxInt64
	var maxTime int64 = math.MinInt64

	for _, rule := range rules {
		for t := start; t <= end; t += evalInterval {
			vector, err := queryFunc(context.Background(), rule.vector.String(), timestamp.Time(t))
			if err != nil {
				level.Warn(logger).Log("err", err)
				continue
			}
			for _, sample := range vector {
				lb := labels.NewBuilder(sample.Metric)
				lb.Set(labels.MetricName, rule.name)

				for _, l := range rule.lset {
					lb.Set(l.Name, l.Value)
				}
				mss = append(mss, &tsdb.MetricSample{Labels: lb.Labels(), Value: sample.V, TimestampMs: sample.T})

				// update the samples time range
				minTime = min(minTime, sample.T)
				maxTime = max(maxTime, sample.T)

				if len(mss) == maxSamples {
					blockID, err := tsdb.CreateBlock(mss, dest, minTime, maxTime, logger)
					if err != nil {
						level.Error(logger).Log("msg", "failed to create block", "err", err)
						return
					}

					minTime = math.MaxInt64
					maxTime = math.MinInt64
					mss = mss[:0]
					level.Info(logger).Log("msg", "create block successfully", "block", blockID)
				}
			}
		}
	}

	// flush the remaining samples
	if len(mss) > 0 {
		blockID, err := tsdb.CreateBlock(mss, dest, minTime, maxTime, logger)
		if err != nil {
			level.Error(logger).Log("msg", "failed to create block", "err", err)
			return
		}
		level.Info(logger).Log("msg", "create block successfully", "block", blockID)
	}

	return
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}
