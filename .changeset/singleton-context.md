---
default: patch
---

Add `SingletonContext` to singleton factory functions for graceful shutdown.

Singleton factories now receive a `SingletonContext` which provides opt-in graceful shutdown. By calling `ctx.cancellation()`, the singleton commits to observing the cancellation token and returning when cancelled. The runtime will wait for singletons that opt-in; singletons that don't will be force-cancelled.

**Key behavior:** When a shard is lost (node failure or rebalancing), the runtime now waits for managed singletons to complete gracefully before allowing another node to start a new instance. This ensures the "exactly one singleton" guarantee is maintained during failover.

**Usage:**

```rust
use cruster::singleton::{register_singleton, SingletonContext};

register_singleton(&*sharding, "my-singleton", |ctx: SingletonContext| async move {
    // Opt-in to graceful shutdown
    let cancel = ctx.cancellation();
    
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                // Graceful shutdown - clean up resources
                break;
            }
            _ = do_work() => {}
        }
    }
    Ok(())
}).await?;
```

**API:**

- `ctx.cancellation()` - Returns a `CancellationToken` and opts the singleton into graceful shutdown. The runtime will wait for the singleton to return instead of force-cancelling it.
- `ctx.is_cancelled()` - Check if cancellation was requested without opting in to manage it.

**Breaking Change:** Singleton factory closures must now accept a `SingletonContext` parameter. For singletons that don't need graceful shutdown, simply ignore the parameter with `|_ctx|`.
