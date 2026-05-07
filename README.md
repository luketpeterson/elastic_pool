# ElasticPool

`ElasticPool` is a worker pool for Elixir, designed to manage finite costly
resources with substantial startup latency.

It uses a module-based API to configure the scaling policy, using one of the supplied algorithms or a custom behaviour you can implement.

It uses a macro-based approach to provide compile-time validation and monomorphized dispatch.

## Scaling Policies

ElasticPool separates the pool implementation from the scaling decision logic so you can choose the policy that matches your situation.

The right scaling behavior is dictated by considerations like:
- the level of acceptable job latency
- the cost of over-provisioning workers
- the lag needed to spin up a worker
- etc.

See `ElasticPool.ScalingPolicy` for the built-in policies as well as implementing a custom policy.

## Configuration

`ElasticPool` separates configuration into compile-time macro options and
runtime `start_link/1` options.

See the complete set of [compile-time macro options](ElasticPool.html#module-macro-options)
and [runtime options](ElasticPool.html#module-runtime-options).

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

## API Reference

### Core API

- `ElasticPool` - Pool definition macro and runtime pool API
- `ElasticPool.Worker` - Public behaviour and `use` macro for workers
- `ElasticPool.ScalingPolicy` - Behaviour for custom scaling policies

### Provided Policies

- `ElasticPool.Policies.ErlangC` - Predictive queueing-theory-based scaling policy
- `ElasticPool.Policies.Null` - Fixed-target no-op scaling policy
- `ElasticPool.Policies.Threshold` - Built-in reactive scaling policy

## Future Features

One natural next step is back-pressure driven by `waiting_clients`.

When the pool is saturated and the checkout queue keeps growing, ElasticPool
could expose configurable overload behavior such as bounded waiting, fast
failure, or caller-side throttling instead of allowing demand to accumulate
without bound.
