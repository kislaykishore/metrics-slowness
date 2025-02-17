package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/googlecloudplatform/gcsfuse/v2/common"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"golang.org/x/sync/errgroup"

	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var (
	fsOpsMeter    = otel.Meter("fs_op")
	fsOpsCount, _ = fsOpsMeter.Int64Counter("fs/ops_count", metric.WithDescription("The cumulative number of ops processed by the file system."))
	workers       = flag.Int("workers", 128, "Concurrent workers")
	timeout       = flag.Duration("timeout", 5*time.Second, "Time to run the benchmark")
)

type ShutdownFn func(ctx context.Context) error

// JoinShutdownFunc combines the provided shutdown functions into a single function.
func JoinShutdownFunc(shutdownFns ...ShutdownFn) ShutdownFn {
	return func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFns {
			if fn == nil {
				continue
			}
			err = errors.Join(err, fn(ctx))
		}
		return err
	}
}

const serviceName = "test"
const metricsPort = 9181

var allowedMetricPrefixes = []string{"fs/", "gcs/", "file_cache/"}

// SetupOTelMetricExporters sets up the metrics exporters
func SetupOTelMetricExporters(ctx context.Context) (shutdownFn common.ShutdownFn) {
	shutdownFns := make([]common.ShutdownFn, 0)
	options := make([]sdkmetric.Option, 0)

	opts, shutdownFn := setupPrometheus(metricsPort)
	options = append(options, opts...)
	shutdownFns = append(shutdownFns, shutdownFn)

	res, err := getResource(ctx)
	if err != nil {
		fmt.Printf("Error while fetching resource: %v\n", err)
	} else {
		options = append(options, sdkmetric.WithResource(res))
	}

	options = append(options, sdkmetric.WithView(dropDisallowedMetricsView))

	meterProvider := sdkmetric.NewMeterProvider(options...)
	shutdownFns = append(shutdownFns, meterProvider.Shutdown)

	otel.SetMeterProvider(meterProvider)

	return common.JoinShutdownFunc(shutdownFns...)
}

// dropUnwantedMetricsView is an OTel View that drops the metrics that don't match the allowed prefixes.
func dropDisallowedMetricsView(i sdkmetric.Instrument) (sdkmetric.Stream, bool) {
	s := sdkmetric.Stream{Name: i.Name, Description: i.Description, Unit: i.Unit}
	for _, prefix := range allowedMetricPrefixes {
		if strings.HasPrefix(i.Name, prefix) {
			return s, true
		}
	}
	s.Aggregation = sdkmetric.AggregationDrop{}
	return s, true
}

func setupPrometheus(port int64) ([]sdkmetric.Option, common.ShutdownFn) {
	if port <= 0 {
		return nil, nil
	}
	exporter, err := prometheus.New(prometheus.WithoutUnits(), prometheus.WithoutCounterSuffixes(), prometheus.WithoutScopeInfo(), prometheus.WithoutTargetInfo())
	if err != nil {
		fmt.Printf("Error while creating prometheus exporter:%v\n", err)
		return nil, nil
	}
	shutdownCh := make(chan context.Context)
	done := make(chan interface{})
	go serveMetrics(port, shutdownCh, done)
	return []sdkmetric.Option{sdkmetric.WithReader(exporter)}, func(ctx context.Context) error {
		shutdownCh <- ctx
		close(shutdownCh)
		<-done
		close(done)
		return nil
	}
}

func serveMetrics(port int64, shutdownCh <-chan context.Context, done chan<- interface{}) {
	//fmt.Printf("Serving metrics at localhost:%d/metrics\n", port)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	prometheusServer := &http.Server{
		Addr:           fmt.Sprintf(":%d", port),
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	go func() {
		if err := prometheusServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Failed to start Prometheus server: %v\n", err)
		}
	}()
	go func() {
		ctx := <-shutdownCh
		defer func() { done <- true }()
		//fmt.Println("Shutting down Prometheus exporter.")
		if err := prometheusServer.Shutdown(ctx); err != nil {
			fmt.Printf("Error while shutting down Prometheus exporter:%v\n", err)
			return
		}
		//fmt.Println("Prometheus exporter shutdown")
	}()
	//fmt.Println("Prometheus collector exporter started")
}

func getResource(ctx context.Context) (*resource.Resource, error) {
	return resource.New(ctx,
		// Use the GCP resource detector to detect information about the GCP platform
		resource.WithDetectors(gcp.NewDetector()),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(common.GetVersion()),
		),
	)
}

func main() {
	flag.Parse()
	ctx := context.Background()
	shutdown := SetupOTelMetricExporters(ctx)
	defer shutdown(ctx)
	ctx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(*timeout))
	defer cancelFunc()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < *workers; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				default:
					fsOpsCount.Add(ctx, 1)
				}
			}
		})
	}
	if err := g.Wait(); err != nil {
		panic(fmt.Errorf("error: %v", err))
	}
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", metricsPort))
	if err != nil {
		panic(fmt.Errorf("error: %v", err))
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	for _, s := range strings.Split(string(body), "\n") {
		if !strings.Contains(s, "fs_ops_count") || strings.Contains(s, "TYPE") || strings.Contains(s, "HELP") {
			continue
		}
		s = strings.ReplaceAll(s, "fs_ops_count ", "")
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%d->%d\n", *workers, int64(v/timeout.Seconds()))
		return
	}
}
