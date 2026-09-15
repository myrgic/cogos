// telemetry.go — OpenTelemetry tracing and metrics for the v3 kernel
//
// Initializes a tracer and meter provider with OTLP HTTP exporters.
// Degrades gracefully to no-op when no collector is available.
//
// Environment variables:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT — collector endpoint (default: http://localhost:4318)
//	OTEL_SERVICE_NAME           — service name (default: cogos)
//
// Usage:
//
//	shutdown := initTelemetry(ctx)
//	defer shutdown(ctx)
//	// Use otel.Tracer("cogos") and otel.Meter("cogos") anywhere.
package engine

import (
	"context"
	"log/slog"
	"net"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
)

// Instruments holds the OTel metric instruments used across the kernel.
var instruments struct {
	ChatRequests     metric.Int64Counter
	ContextTokens    metric.Int64Histogram
	DocsInjected     metric.Int64Histogram
	TurnsEvicted     metric.Int64Counter
	InferenceLatency metric.Float64Histogram
	InferenceTokens  metric.Int64Histogram
}

// initTelemetry sets up OTel tracing and metrics with OTLP HTTP exporters.
// Returns a shutdown function that flushes and stops the providers.
// On failure (no collector, etc.), installs no-op providers and continues.
func initTelemetry(ctx context.Context) func(context.Context) {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "cogos"
	}

	res, _ := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
		resource.WithAttributes(attribute.String("cogos.version", Version)),
	)

	var shutdowns []func(context.Context) error

	// Check if an OTLP collector is reachable before enabling exporters.
	// This avoids noisy retry logs when no collector is running.
	collectorReachable := probeCollector()

	if collectorReachable {
		// Trace exporter.
		traceExp, err := otlptracehttp.New(ctx)
		if err != nil {
			slog.Debug("telemetry: trace exporter unavailable", "err", err)
		} else {
			tp := sdktrace.NewTracerProvider(
				sdktrace.WithBatcher(traceExp),
				sdktrace.WithResource(res),
			)
			otel.SetTracerProvider(tp)
			shutdowns = append(shutdowns, tp.Shutdown)
			slog.Info("telemetry: tracing enabled")
		}

		// Metric exporter.
		metricExp, err := otlpmetrichttp.New(ctx)
		if err != nil {
			slog.Debug("telemetry: metric exporter unavailable", "err", err)
		} else {
			mp := sdkmetric.NewMeterProvider(
				sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(30*time.Second))),
				sdkmetric.WithResource(res),
			)
			otel.SetMeterProvider(mp)
			shutdowns = append(shutdowns, mp.Shutdown)
			slog.Info("telemetry: metrics enabled")
		}
	} else {
		slog.Info("telemetry: no OTLP collector found, using noop (traces and metrics disabled)")
	}

	// Register instruments.
	meter := otel.Meter("cogos")
	instruments.ChatRequests, _ = meter.Int64Counter("cogos.chat.requests",
		metric.WithDescription("Total chat completion requests"))
	instruments.ContextTokens, _ = meter.Int64Histogram("cogos.context.tokens",
		metric.WithDescription("Total tokens assembled per request"))
	instruments.DocsInjected, _ = meter.Int64Histogram("cogos.context.docs_injected",
		metric.WithDescription("CogDocs injected per request"))
	instruments.TurnsEvicted, _ = meter.Int64Counter("cogos.context.turns_evicted",
		metric.WithDescription("Conversation turns evicted due to budget"))
	instruments.InferenceLatency, _ = meter.Float64Histogram("cogos.inference.latency_ms",
		metric.WithDescription("Inference latency in milliseconds"))
	instruments.InferenceTokens, _ = meter.Int64Histogram("cogos.inference.tokens",
		metric.WithDescription("Inference output tokens"))

	return func(ctx context.Context) {
		for _, fn := range shutdowns {
			_ = fn(ctx)
		}
	}
}

// probeCollector does a quick TCP dial to check if an OTLP collector is listening.
func probeCollector() bool {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4318"
	}
	// Strip scheme if present.
	for _, prefix := range []string{"https://", "http://"} {
		if len(endpoint) > len(prefix) && endpoint[:len(prefix)] == prefix {
			endpoint = endpoint[len(prefix):]
		}
	}
	conn, err := net.DialTimeout("tcp", endpoint, 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
