defmodule ElasticPool.ScalingPolicy do
  @moduledoc """
  A behaviour for defining scaling policies.
  """

  @type stats :: %{
          total_workers: integer(),
          available_workers: integer(),
          peak_workers: integer(),
          waiting_clients: integer()
        }

  @doc """
  Initializes the policy state.
  Receives a map containing:
    * `:policy_opts` - Choices specific to this policy.
    * `:pool_config` - Structural metadata about the pool (e.g., max_workers).
  """
  @callback init(opts :: map()) :: policy_state :: term()

  @callback handle_stats(stats :: stats(), policy_state :: term()) ::
              {:scale_up, count :: pos_integer(), new_state :: term()}
              | {:scale_down, count :: pos_integer(), new_state :: term()}
              | {:none, new_state :: term()}
end
