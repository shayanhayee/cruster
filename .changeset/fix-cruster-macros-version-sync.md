---
default: patch
---

Fix cruster-macros version sync in release workflow

Move `cruster-macros` dependency to workspace dependencies and configure knope to automatically bump its version during releases. This ensures the `cruster-macros` version stays in sync with the workspace version when publishing to crates.io.

- Add `cruster-macros` to `[workspace.dependencies]` in root `Cargo.toml`
- Update `crates/cruster/Cargo.toml` to use `cruster-macros = { workspace = true }`
- Configure `knope.toml` to version the `cruster-macros` dependency in workspace
