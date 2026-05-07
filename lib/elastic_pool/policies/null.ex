defmodule ElasticPool.Policies.Null do
  @moduledoc """
  A no-op scaling policy that never changes the worker target.

  This policy leaves the pool target fixed at the initial value established
  during startup. It ignores all scaling events and returns `:no_change` for
  every event.

  Use this when you want `ElasticPool` to behave like a fixed-size pool.
  """

  @behaviour ElasticPool.ScalingPolicy

  @impl true
  @spec init(map()) :: map()
  def init(_opts), do: %{}

  @impl true
  @spec handle_event(
          ElasticPool.ScalingPolicy.event(),
          ElasticPool.ScalingPolicy.pool_name(),
          map()
        ) :: {:no_change, map()}
  def handle_event(_event, _pool, state), do: {:no_change, state}
end
