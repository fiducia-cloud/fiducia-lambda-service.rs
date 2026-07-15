# workflow

The workflow engine: run lifecycle (create → step → complete/cancel/fail),
durable run/signal storage, and the event emission points that publish
`dd.remote.workflows.events`. The store bounds signal delivery (see
`signal_delivery_is_bounded`); engine state transitions are the unit-tested
core.
