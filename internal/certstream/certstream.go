package certstream

// The certstream package provides the main entry point for the certstream-server-go application.
// It initializes the webserver and the watcher for the certificate transparency logs.
// It also handles signals for graceful shutdown of the server.

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/d-Rickyy-b/certstream-server-go/internal/certificatetransparency"
	"github.com/d-Rickyy-b/certstream-server-go/internal/config"
	"github.com/d-Rickyy-b/certstream-server-go/internal/deduplicator"
	"github.com/d-Rickyy-b/certstream-server-go/internal/disk"
	"github.com/d-Rickyy-b/certstream-server-go/internal/kafka"
	"github.com/d-Rickyy-b/certstream-server-go/internal/metrics"
	"github.com/d-Rickyy-b/certstream-server-go/internal/web"
)

// shutdownDrainTimeout bounds how long Start waits for each sink to flush on shutdown, so
// an unreachable sink (e.g. a down Kafka broker) cannot hang the process indefinitely.
const shutdownDrainTimeout = 30 * time.Second

type Certstream struct {
	webserver     *web.Server
	metricsServer *web.Server
	watcher       *certificatetransparency.Watcher
	deduplicator  *deduplicator.Deduplicator
	config        config.Config

	// diskDone/kafkaDone are closed by their sinks once they have drained and flushed.
	// The graceful-shutdown drain at the tail of Start waits on them.
	diskDone  <-chan struct{}
	kafkaDone <-chan struct{}
}

func NewRawCertstream(config config.Config) *Certstream {
	cs := Certstream{}
	cs.config = config

	return &cs
}

// NewCertstreamServer creates a new Certstream server from a config struct.
func NewCertstreamServer(config config.Config) (*Certstream, error) {
	cs := NewRawCertstream(config)

	// Initialize the webserver used for the websocket server
	webserver := web.NewWebsocketServer(
		config.Webserver.ListenAddr,
		config.Webserver.ListenPort,
		config.Webserver.CertPath,
		config.Webserver.CertKeyPath,
	)
	cs.webserver = webserver
	cs.watcher = certificatetransparency.NewWatcher()

	// Setup metrics server
	cs.setupMetrics(webserver)

	return cs, nil
}

// NewCertstreamFromConfigFile creates a new Certstream server from a config file.
func NewCertstreamFromConfigFile(configPath string) (*Certstream, error) {
	conf, err := config.ReadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("error reading config: %w", err)
	}

	return NewCertstreamServer(conf)
}

// setupMetrics configures the webserver to handle prometheus metrics according to the config.
func (cs *Certstream) setupMetrics(webserver *web.Server) {
	if cs.config.Prometheus.Enabled {
		// If prometheus is enabled, and interface is either unconfigured or same as webserver config, use existing webserver
		if (cs.config.Prometheus.ListenAddr == "" || cs.config.Prometheus.ListenAddr == cs.config.Webserver.ListenAddr) &&
			(cs.config.Prometheus.ListenPort == 0 || cs.config.Prometheus.ListenPort == cs.config.Webserver.ListenPort) {
			log.Println("Starting prometheus server on same interface as webserver")
			webserver.RegisterPrometheus(cs.config.Prometheus.MetricsURL, metrics.Prometheus.Write)
		} else {
			log.Println("Starting prometheus server on new interface")

			cs.metricsServer = web.NewMetricsServer(
				cs.config.Prometheus.ListenAddr,
				cs.config.Prometheus.ListenPort,
				cs.config.Prometheus.CertPath,
				cs.config.Prometheus.CertKeyPath,
			)
			cs.metricsServer.RegisterPrometheus(cs.config.Prometheus.MetricsURL, metrics.Prometheus.Write)
		}
	}
}

// Start starts the webserver and the watcher.
// This is a blocking function that will run until the server is stopped.
func (cs *Certstream) Start() {
	log.Printf("Starting certstream-server-go v%s\n", config.Version)

	// handle signals in a separate goroutine
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go signalHandler(signals, cs.Stop)

	// If there is no watcher initialized, create a new one
	if cs.watcher == nil {
		cs.watcher = certificatetransparency.NewWatcher()
	}

	// Start webserver and metrics server
	if cs.webserver == nil {
		log.Fatalln("Webserver not initialized! Exiting...")
	}

	go cs.webserver.Start()

	if cs.metricsServer != nil {
		go cs.metricsServer.Start()
	}

	// Start the disk logger before the watcher, so the entry channel exists
	// when the watcher's certHandler wires up its destination channels.
	if cs.config.DiskLogger.Enabled {
		cs.diskDone = disk.StartLogger(cs.config.DiskLogger.LogDirectory, cs.config.DiskLogger.Type, cs.config.DiskLogger.Rotation)
	}

	// Build the shared domain deduplicator before the watcher starts, so the fan-out
	// stage is ready when certHandler snapshots its destinations. It is applied as a
	// middleware ahead of the fan-out to the per-domain sinks (Kafka FERRET_DOMAIN and
	// disk DOMAINS_ONLY), deduplicated once against a single shared DB; all other sinks
	// receive raw entries. A failure here is never fatal: we log it and run without
	// deduplication.
	kafkaConfigEligibleForDedupe := cs.config.Kafka.Enabled && cs.config.Kafka.Format == string(kafka.FormatFerretDomain)
	diskConfigEligibleForDedupe := cs.config.DiskLogger.Enabled && cs.config.DiskLogger.Type == disk.DISK_LOG_DOMAINS_ONLY

	if cs.config.Deduplicator.Enabled && (kafkaConfigEligibleForDedupe || diskConfigEligibleForDedupe) {
		dedup, err := deduplicator.New(buildDeduplicatorOptions(cs.config))
		if err != nil {
			log.Printf("Deduplicator disabled: failed to initialize: %v\n", err)
		} else {
			cs.deduplicator = dedup
			cs.watcher.SetDedup(dedup, diskConfigEligibleForDedupe, kafkaConfigEligibleForDedupe)
		}
	}

	// Start the Kafka publisher before the watcher for the same ordering reason.
	// A failure here is never fatal: we log it and keep serving websocket/disk.
	if cs.config.Kafka.Enabled {
		done, err := kafka.StartPublisher(buildKafkaOptions(cs.config))
		if err != nil {
			log.Printf("Kafka publisher disabled: failed to initialize: %v\n", err)
		} else {
			cs.kafkaDone = done
		}
	}

	// Start the watcher - this is a blocking function that returns only once the watcher
	// has been stopped (via Stop) and has closed its entry channel.
	cs.watcher.Start()

	// Graceful shutdown: the closed entry channel cascades through certHandler -> the
	// dedup stage -> the sink channels. Wait for every sink to drain its buffer and flush
	// before closing the deduplicator and returning, so nothing buffered in the pipeline
	// (e.g. Kafka's final in-flight batch) is lost.
	cs.drainSinks()
}

// drainSinks waits for the disk and Kafka sinks to finish draining and flushing
func (cs *Certstream) drainSinks() {
	// Kafka retries forever, but dont want to wait forever
	ctx, cancel := context.WithTimeout(context.Background(), shutdownDrainTimeout)
	defer cancel()

	waitForSink(ctx, cs.diskDone, "disk logger")
	waitForSink(ctx, cs.kafkaDone, "kafka publisher")

	if cs.deduplicator != nil {
		if err := cs.deduplicator.Close(); err != nil {
			log.Printf("Error closing deduplicator: %v\n", err)
		}
	}
}

func waitForSink(ctx context.Context, done <-chan struct{}, name string) {
	if done == nil {
		return
	}

	select {
	case <-done:
		log.Printf("%s drained and flushed\n", name)
	case <-ctx.Done():
		log.Printf("Timed out after %s waiting for %s to flush; some buffered data may be lost\n", shutdownDrainTimeout, name)
	}
}

func (cs *Certstream) Stop() {
	if cs.watcher != nil {
		cs.watcher.Stop()
	}

	if cs.webserver != nil {
		cs.webserver.Stop()
	}

	if cs.metricsServer != nil {
		cs.metricsServer.Stop()
	}
}

// CreateIndexFile creates the index file for the certificate transparency logs.
// It gets only called when the CLI flag --create-index-file is set.
func (cs *Certstream) CreateIndexFile(outFile string) error {
	// If there is no watcher initialized, create a new one
	if cs.watcher == nil {
		cs.watcher = certificatetransparency.NewWatcher()
	}

	err := cs.watcher.CreateIndexFile(outFile)
	if err != nil {
		return fmt.Errorf("error creating index file: %w", err)
	}

	return nil
}

// buildDeduplicatorOptions maps the Deduplicator section of the config into the
// primitive deduplicator.Options struct.
func buildDeduplicatorOptions(cfg config.Config) deduplicator.Options {
	d := cfg.Deduplicator

	return deduplicator.Options{
		DBPath:        d.DBPath,
		Retention:     time.Duration(d.RetentionDays) * 24 * time.Hour,
		PurgeInterval: time.Duration(d.PurgeIntervalHours) * time.Hour,
		CacheSize:     d.CacheSize,
	}
}

// buildKafkaOptions maps the Kafka section of the config into the primitive
// kafka.Options struct (which the config package cannot reference directly
// without creating an import cycle).
func buildKafkaOptions(cfg config.Config) kafka.Options {
	k := cfg.Kafka

	mechanism := ""
	if k.Auth.Enabled {
		mechanism = k.Auth.Mechanism
	}

	return kafka.Options{
		Brokers:               k.Brokers,
		Topic:                 k.Topic,
		Format:                kafka.Format(k.Format),
		Compression:           k.Compression,
		ClientID:              k.ClientID,
		Linger:                time.Duration(k.LingerMs) * time.Millisecond,
		BatchMaxBytes:         k.BatchMaxBytes,
		BatchMaxRecords:       k.BatchMaxRecords,
		ChannelBuffer:         k.ChannelBuffer,
		SASLMechanism:         mechanism,
		Username:              k.Auth.Username,
		Password:              k.Auth.Password,
		TLSEnabled:            k.TLS.Enabled,
		TLSInsecureSkipVerify: k.TLS.InsecureSkipVerify,
		TLSCACertPath:         k.TLS.CACert,
	}
}

// signalHandler listens for signals in order to gracefully shut down the server.
// Executes the callback function when a signal is received.
func signalHandler(signals chan os.Signal, callback func()) {
	log.Println("Listening for signals...")

	sig := <-signals
	log.Printf("Received signal %v. Shutting down...\n", sig)
	callback()
}
