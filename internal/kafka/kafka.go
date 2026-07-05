package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"google.golang.org/protobuf/proto"

	"github.com/d-Rickyy-b/certstream-server-go/internal/eventspb"
	"github.com/d-Rickyy-b/certstream-server-go/internal/models"
)

// CertStreamEntryChan carries entries from certHandler to the publisher goroutine.
var CertStreamEntryChan chan models.Entry

type Format string

const (
	FormatFull         Format = "FULL"
	FormatLite         Format = "LITE"
	FormatDomainsOnly  Format = "DOMAINS_ONLY"
	FormatFerretDomain Format = "FERRET_DOMAIN"
)

type Options struct {
	Brokers         []string
	Topic           string
	Format          Format
	Compression     string // none, gzip, snappy, lz4, zstd
	ClientID        string
	Linger          time.Duration // time-based batch flush interval (from linger_ms)
	BatchMaxBytes   int32
	BatchMaxRecords int
	ChannelBuffer   int

	SASLMechanism string // "", plain, scram-sha-256, scram-sha-512
	Username      string
	Password      string

	TLSEnabled            bool
	TLSInsecureSkipVerify bool
	TLSCACertPath         string
}

func StartPublisher(opts Options) error {
	client, err := newClient(opts)
	if err != nil {
		return err
	}

	if CertStreamEntryChan == nil {
		CertStreamEntryChan = make(chan models.Entry, opts.ChannelBuffer)
	}

	go publishEntries(client, opts)

	log.Printf("Kafka publisher started: brokers=%v topic=%q format=%s compression=%s\n",
		opts.Brokers, opts.Topic, opts.Format, opts.Compression)

	return nil
}

func newClient(opts Options) (*kgo.Client, error) {
	kopts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.DefaultProduceTopic(opts.Topic),
		kgo.ProducerBatchCompression(compressionCodec(opts.Compression)),
		// Surface background (re)connection problems without being fatal.
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelWarn, nil)),
		// Keep the idempotent producer default (acks=all) for durable writes -
		// do NOT set RequiredAcks, which would conflict with idempotency.
	}

	if opts.ClientID != "" {
		kopts = append(kopts, kgo.ClientID(opts.ClientID))
	}

	if opts.BatchMaxBytes > 0 {
		kopts = append(kopts, kgo.ProducerBatchMaxBytes(opts.BatchMaxBytes))
	}

	mech, err := saslMechanism(opts)
	if err != nil {
		return nil, err
	}

	if mech != nil {
		kopts = append(kopts, kgo.SASL(mech))
	}

	if opts.TLSEnabled {
		tlsConfig, tlsErr := buildTLSConfig(opts)
		if tlsErr != nil {
			return nil, tlsErr
		}

		kopts = append(kopts, kgo.DialTLSConfig(tlsConfig))
	}

	return kgo.NewClient(kopts...)
}

// publishEntries drains the CertStreamEntryChan, batching records and flushing them
// to Kafka either when the batch reaches BatchMaxRecords or when the linger ticker fires.
// ProduceSync blocks until the broker acknowledges the batch,
func publishEntries(client *kgo.Client, opts Options) {
	flushInterval := opts.Linger
	if flushInterval <= 0 {
		flushInterval = time.Second
	}

	maxBatch := opts.BatchMaxRecords
	if maxBatch <= 0 {
		maxBatch = 50
	}

	batch := make([]*kgo.Record, 0, maxBatch)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		// ProduceSync retries retriable errors indefinitely (no delivery
		// deadline), so this blocks while the broker is unavailable rather than
		// dropping. A returned error means a non-retriable failure (e.g. record
		// too large, topic ACL denied) - log it loudly and move on.
		if err := client.ProduceSync(context.Background(), batch...).FirstErr(); err != nil {
			log.Printf("kafka: failed to produce batch of %d records: %v\n", len(batch), err)
		}

		batch = batch[:0]
	}

	for {
		select {
		case entry, ok := <-CertStreamEntryChan:
			if !ok {
				flush()
				client.Close()

				return
			}

			batch = append(batch, recordsFor(entry, opts.Format)...)
			if len(batch) >= maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// recordFor encodes a single entry into a Kafka record according to the format.
// The topic is supplied by kgo.DefaultProduceTopic, so it is left unset here.
func recordsFor(entry models.Entry, format Format) []*kgo.Record {
	if format == FormatFerretDomain {
		return ferretDomainRecords(entry)
	}

	var value []byte

	switch format {
	case FormatDomainsOnly:
		value = entry.JSONDomains()
	case FormatLite:
		value = entry.JSONLite()
	case FormatFull:
		fallthrough
	default:
		value = entry.JSON()
	}

	return []*kgo.Record{kgo.SliceRecord(value)}
}

func ferretDomainRecords(entry models.Entry) []*kgo.Record {
	records := make([]*kgo.Record, 0, len(entry.Data.LeafCert.AllDomains))

	now := time.Now().Unix()
	for _, domain := range entry.Data.LeafCert.AllDomains {
		msg := &eventspb.DomainInput{
			Domain:       domain,
			Source:       "CERTSTREAM",
			DiscoveredAt: min(entry.Data.LeafCert.NotBefore, now),
			EnqueuedAt:   now,
		}

		payload, err := proto.Marshal(msg)
		if err != nil {
			log.Printf("kafka: failed to marshal DomainInput for %q: %v\n", domain, err)
			continue
		}

		records = append(records, kgo.SliceRecord(payload))
	}

	return records
}

// compressionCodec maps a config string to a franz-go compression codec.
func compressionCodec(name string) kgo.CompressionCodec {
	switch name {
	case "none":
		return kgo.NoCompression()
	case "gzip":
		return kgo.GzipCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	case "snappy":
		fallthrough
	default:
		return kgo.SnappyCompression()
	}
}

// saslMechanism builds the SASL mechanism for the configured auth, or returns
// (nil, nil) when no SASL mechanism is configured.
func saslMechanism(opts Options) (sasl.Mechanism, error) {
	switch opts.SASLMechanism {
	case "":
		return nil, nil
	case "plain":
		return plain.Auth{User: opts.Username, Pass: opts.Password}.AsMechanism(), nil
	case "scram-sha-256":
		return scram.Auth{User: opts.Username, Pass: opts.Password}.AsSha256Mechanism(), nil
	case "scram-sha-512":
		return scram.Auth{User: opts.Username, Pass: opts.Password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unknown SASL mechanism %q", opts.SASLMechanism)
	}
}

// buildTLSConfig creates a TLS config, optionally trusting an extra CA cert.
func buildTLSConfig(opts Options) (*tls.Config, error) {
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}

	if opts.TLSCACertPath != "" {
		ca, readErr := os.ReadFile(opts.TLSCACertPath)
		if readErr != nil {
			return nil, fmt.Errorf("failed to read Kafka CA cert %q: %w", opts.TLSCACertPath, readErr)
		}

		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("failed to parse Kafka CA cert %q", opts.TLSCACertPath)
		}
	}

	return &tls.Config{
		RootCAs:            pool,
		InsecureSkipVerify: opts.TLSInsecureSkipVerify, //nolint:gosec // opt-in via config
		MinVersion:         tls.VersionTLS12,
	}, nil
}
