defmodule ElasticPool.ScalingPolicy do
  @moduledoc """
  A behaviour for defining stateful scaling policies.
  """

  @type event :: :checkout_success | :checkout_failed | :checkin | :worker_ready | :heartbeat

  @doc "Initialize the policy state"
  @callback init(opts :: map()) :: state :: term()

  @doc """
  Evaluates the scaling target based on an event.
  Returns {new_target, new_state}.
  """
  @callback handle_event(
              event :: event(),
              pool_name :: atom(),
              state :: term()
            ) :: {target :: pos_integer(), new_state :: term()}
end
