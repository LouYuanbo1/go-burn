package burn

import "log"

type OptionFunc func(*Tester)

func WithLogger(logger *log.Logger) OptionFunc {
	return func(t *Tester) { t.logger = logger }
}

func WithConcurrencyLimit(limit int) OptionFunc {
	return func(t *Tester) { t.concurrencyLimit = limit }
}

func WithStats(stats *Stats) OptionFunc {
	return func(t *Tester) { t.stats = stats }
}
