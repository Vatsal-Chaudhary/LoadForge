# Live stream source

The API server subscribes each SSE connection to
`loadforge.metrics.<run_id>.*`, the existing raw metrics subject consumed by
the Aggregator. It folds batches into a one-second in-memory window and emits
only the required summary event.

This was chosen instead of adding a new Aggregator publication contract: it
keeps the existing producer/consumer protocol unchanged and ensures SSE
delivery does not depend on a second summary publisher. Each subscription is
unsubscribed synchronously when the request context is cancelled, so a client
disconnect does not leave a goroutine or NATS subscription behind.
