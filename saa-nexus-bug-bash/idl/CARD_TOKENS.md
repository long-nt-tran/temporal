# Card token chaos vocabulary

`cardToken` on `ChargeRequest`/`SubscriptionRequest` selects deterministic
chaos behavior. Most tokens are interpreted *inside the backing activity*
(`ts/src/chaos.ts::interpretCardToken`) — these exercise the activity's own
retry policy, not the Nexus protocol/dispatch layer. One token
(`tok_handler_retryable_error`) is checked in the *handler* itself
(`ts/src/handler.ts`), before any activity ever starts.

| Token | Behavior |
|---|---|
| `tok_ok` | Succeeds on the first attempt. |
| `tok_decline` | RPC succeeds; business `status` is `declined`. |
| `tok_pending_review` | RPC succeeds; business `status` is `pending_review`. |
| `tok_retry_{N}_then_ok` | Throws a retryable `ApplicationFailure` on attempts `1..N`, succeeds on attempt `N+1`. e.g. `tok_retry_3_then_ok`. Use with `orderId` reuse to watch the retry converge to one Activity Execution. This is the activity's own retry policy — it does **not** exercise any Nexus-level circuit breaker (see `tok_handler_retryable_error` below for that). |
| `tok_retry_forever` | Always throws a retryable `ApplicationFailure`. Never succeeds — exhausts the activity retry policy every time; will always time out at whatever `scheduleToCloseTimeout` the caller sets. Same caveat as above — no breaker involvement. |
| `tok_fail_nonretryable` | Throws a non-retryable `ApplicationFailure` on the first attempt. |
| `tok_timeout_{N}s` | Sleeps `N` seconds without heartbeating. Pair with a `scheduleToCloseTimeout`/`startToCloseTimeout` shorter than `N` to observe a timeout instead of a completion. e.g. `tok_timeout_30s`. |
| `tok_hang` | Loops forever, heartbeating every second, until cancelled or the worker restarts. Use with `cancelBehavior` and `runLongTask` for the cancellation and worker-restart drills. |
| `tok_handler_retryable_error` (`chargeOrder` only) | Throws `new nexus.HandlerError('INTERNAL', ...)` synchronously from the handler's `start()`, *before* starting any backing activity. `INTERNAL` is retryable by default per `nexus-rpc`'s `HandlerError.retryable` (so are `UNAVAILABLE`/`RESOURCE_EXHAUSTED`/`UPSTREAM_TIMEOUT`/`REQUEST_TIMEOUT`; `BAD_REQUEST`/`NOT_FOUND`/etc. are not) — this is a genuine Nexus dispatch-level retryable error, the kind a caller→endpoint circuit breaker (scenario 5) would plausibly count. Fails near-instantly (no activity, no timeout wait), unlike every other failing token above. |
| anything else | Treated as an unrecognized card: RPC succeeds, `status` is `declined`, `errorCode` is `unknown_card_token`. |

`cancelBehavior` (on `ChargeRequest` and `LongTaskRequest`) selects how the
activity reacts once a cancel has been requested:

| Value | Behavior |
|---|---|
| `cooperative` (default) | Heartbeats regularly; a cancel request interrupts the next sleep/heartbeat and the activity exits promptly as `CANCELLED`. |
| `heartbeat_ignore_cancel` | Keeps heartbeating (so the server sees liveness and the cancel-requested flag) but swallows the cancellation and runs to completion anyway. |
| `ignore_cancel_entirely` | Never heartbeats and never checks for cancellation; runs to completion (or until the worker is killed) regardless of any cancel request. |
