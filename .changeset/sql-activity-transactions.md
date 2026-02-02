---
default: patch
---

Add support for executing arbitrary SQL within activity transactions.

Activities can now execute custom SQL statements that are committed or rolled back together with entity state changes, ensuring atomicity.

**Usage:**

```rust
#[activity]
async fn transfer(&mut self, to: String, amount: i64) -> Result<(), ClusterError> {
    // State mutation (automatically transactional)
    self.state.balance -= amount;
    
    // Execute arbitrary SQL in the same transaction
    if let Some(tx) = ActivityScope::sql_transaction().await {
        tx.execute(
            sqlx::query("INSERT INTO transfers (...) VALUES ($1, $2, $3)")
                .bind(&self.state.entity_id)
                .bind(&to)
                .bind(amount)
        ).await?;
    }
    
    Ok(())
}
```

**API:**

- `ActivityScope::sql_transaction()` - Returns `Option<SqlTransactionHandle>` (requires `sql` feature)
- `SqlTransactionHandle::execute()` - Execute a SQL statement
- `SqlTransactionHandle::fetch_one()` - Fetch exactly one row
- `SqlTransactionHandle::fetch_optional()` - Fetch zero or one row
- `SqlTransactionHandle::fetch_all()` - Fetch all rows

Returns `None` when using non-SQL storage backends (e.g., memory storage for testing).
