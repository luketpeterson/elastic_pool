# ElasticPool

`ElasticPool` is a worker pool for Elixir, designed to manage finite costly
resources with substantial startup latency.

It uses a module-based API to configure the scaling policy, using one of the supplied algorithms or a custom behaviour you can implement.

It uses a macro-based approach to provide compile-time validation and monomorphized dispatch.

## Configuration

`ElasticPool` separates configuration into two phases.

### 1. Macro Options

These are passed to `use ElasticPool` and are used to generate specialized
code.

- `:worker_handler` - Required worker module implementing `ElasticPool.Worker`
- `:scaling_policy` - Optional scaling policy module. Defaults to
  `ElasticPool.Policies.Threshold`

### 2. Runtime Options

These are passed to your pool's `start_link/1` function.

- `:name` - Name of the pool instance. Defaults to the module name
- `:worker_args` - Arguments passed to the worker module's `init/1` callback
- `:initial_workers` - Pool size at initialization. Defaults to `2`
- `:max_workers` - Absolute ceiling on the number of workers
- `:scaling_policy_opts` - Options passed to the scaling policy
- `:start_timeout` - Time in ms to wait for initial workers to come up
- `:max_restarts` - Maximum number of worker crashes allowed in `:max_period`
- `:max_period` - Time window for `:max_restarts` in seconds
- `:stats_interval` - Time in ms for periodic status telemetry heartbeats.
  Set to `:never` to disable periodic stats

## Example

```elixir
defmodule MyApp.Worker do
  use ElasticPool.Worker

  @impl true
  def handle_work({:echo, value}, _from, state) do
    {:reply, value, state}
  end
end

defmodule MyPool do
  use ElasticPool,
    worker_handler: MyApp.Worker
end

# Start it in your supervision tree with runtime options:
{MyPool, [initial_workers: 5, max_workers: 10]}

# Perform work:
MyPool.call({:echo, "hello"})
```
