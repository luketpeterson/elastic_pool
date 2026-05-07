defmodule ElasticPool.ScalingPolicy do
  @moduledoc """
  A behaviour for defining stateful scaling policies.

  Scaling policies decide how many workers the pool should target as demand
  changes.

  Different workloads benefit from different policies:

  - fixed-size pools when each worker represents a scarce resource
  - reactive policies when simple queue and idle thresholds are sufficient
  - predictive policies when you want to scale from observed demand and task
    speed before clients wait too long

  Built-in policies:

  - `ElasticPool.Policies.Null` - keeps the target fixed
  - `ElasticPool.Policies.Threshold` - reacts to queue pressure and idle reserve
  - `ElasticPool.Policies.ErlangC` - targets a queue-wait budget using
    queueing-theory estimates
  """

  @type pool_name :: atom()
  @type event :: :checkout_success | :checkout_failed | :checkin | :worker_ready | :heartbeat
  @type target_decision :: pos_integer() | :no_change

  @doc "Initialize the policy state"
  @callback init(opts :: map()) :: state :: term()

  @doc """
  Evaluates the scaling target based on an event.
  Returns `{new_target, new_state}`.

  Policies may return `:no_change` as the target to keep the current target unchanged.
  """
  @callback handle_event(
              event :: event(),
              pool_name :: pool_name(),
              state :: term()
            ) :: {target :: target_decision(), new_state :: term()}
end
