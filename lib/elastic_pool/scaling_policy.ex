defmodule ElasticPool.ScalingPolicy do
  @moduledoc """
  A behaviour for defining stateful scaling policies.
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
