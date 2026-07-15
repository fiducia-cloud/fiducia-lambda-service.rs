# Durable workflow engine

The workflow store is the durable source of truth; the engine claims work,
advances one state-machine transition, and publishes lifecycle notifications.
NATS events must follow a committed transition and use the standard envelope.
Cross-replica execution requires fiducia-node leases, fencing, and idempotency.
