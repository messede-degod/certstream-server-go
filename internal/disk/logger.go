package disk

import (
	"log"

	"github.com/d-Rickyy-b/certstream-server-go/internal/disk/filerotate"
	"github.com/d-Rickyy-b/certstream-server-go/internal/models"
)

var (
	CertStreamEntryChan chan models.Entry
)

type DiskLog string

const (
	DISK_LOG_FULL         DiskLog = "FULL"
	DISK_LOG_LITE         DiskLog = "LITE"
	DISK_LOG_DOMAINS_ONLY DiskLog = "DOMAINS_ONLY"
)

func StartLogger(logDirectory string, logType DiskLog, rotation string) <-chan struct{} {
	if CertStreamEntryChan == nil {
		CertStreamEntryChan = make(chan models.Entry, 10_000)
	}

	done := make(chan struct{})
	go logEntries(logDirectory, logType, rotation, done)

	return done
}

func logEntries(logDirectory string, logType DiskLog, rotation string, done chan struct{}) {
	// Signal completion so a graceful shutdown can wait for the final writes to be
	// flushed before exiting.
	defer close(done)

	var logFile *filerotate.RotatableFile
	var err error

	switch rotation {
	case "HOURLY":
		logFile, err = filerotate.New(logDirectory, filerotate.ROTATE_HOURLY)
	case "DAILY":
		fallthrough
	default:
		logFile, err = filerotate.New(logDirectory, filerotate.ROTATE_DAILY)
	}

	if err != nil {
		log.Panic(err)
	}

	for {
		entry, ok := <-CertStreamEntryChan

		if !ok {
			break
		}

		switch logType {
		case DISK_LOG_DOMAINS_ONLY:
			for _, domain := range entry.Data.LeafCert.AllDomains {
				logFile.Write([]byte(domain + "\n"))
			}
		case DISK_LOG_LITE:
			logFile.Write(entry.JSONLite())
		case DISK_LOG_FULL:
			fallthrough
		default:
			logFile.Write(entry.JSON())
		}
	}

	logFile.Close()
}
