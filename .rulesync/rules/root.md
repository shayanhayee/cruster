---
description: "Global context for the Cruster project"
root: true
---

# Cruster

Rust framework for distributed, stateful entity systems with durable workflows.

## Reference Implementation

The original Effect cluster code (TypeScript) can be found in `.repos/effect-smol/packages/effect/src/unstable/cluster`.

## Critical Constraints

- **No auto-commits** - Wait for explicit user approval
- **State access** - Use `self.state` in activities (`&mut self`), not `state_mut()`
- **Trait activities** - Use `#[protected]` for activities callable by entities using the trait
