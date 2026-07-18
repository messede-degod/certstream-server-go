package certificatetransparency

import (
	"context"
	"log"

	"github.com/d-Rickyy-b/certstream-server-go/internal/models"
)

type DomainFilter interface {
	FilterNew(ctx context.Context, domains []string) ([]string, error)
}

// dedupStageBuffer is the size of the buffer in front of the dedup stage. It absorbs
// bursts so the SQLite FilterNew call never stalls certHandler's fan-out.
const dedupStageBuffer = 10_000

func dedupFanout(dedupSinks []chan models.Entry, filter DomainFilter) chan models.Entry {
	in := make(chan models.Entry, dedupStageBuffer)

	if len(dedupSinks) == 0 {
		log.Panic("dedupFanout called with no sinks")
	}

	go func() {
		for entry := range in {
			newDomains, err := filter.FilterNew(context.Background(), entry.Data.LeafCert.AllDomains)

			switch {
			case err != nil:
				// Fail open: forward everything rather than dropping on a dedup error.
			case len(newDomains) == 0:
				continue // nothing new to forward, skip this entry
			default:
				// FilterNew returns a fresh slice, so this reassignment never mutates the
				// backing array of the copies fanned out to the raw sinks.
				entry.Data.LeafCert.AllDomains = newDomains
			}

			for _, ch := range dedupSinks {
				ch <- entry
			}
		}
	}()

	return in
}
