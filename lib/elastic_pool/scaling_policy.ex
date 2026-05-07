defmodule ElasticPool.ScalingPolicy do
  @moduledoc """
  A behaviour for defining stateful scaling policies.

  Scaling policies decide how many workers the pool should target as demand
  changes.

  ## Provided Policies:

  - `ElasticPool.Policies.Null` - Keeps the pool size fixed at the initial worker count
  - `ElasticPool.Policies.Threshold` - Reacts to queue pressure and idle worker count using simple thresholds
  - `ElasticPool.Policies.ErlangC` - Uses queueing theory to target a wait-time budget from observed demand

  ## Custom Policy Example

      defmodule MyApp.StepPolicy do
        @behaviour ElasticPool.ScalingPolicy
        require ElasticPool

        @impl true
        def init(%{pool_config: pool_config}) do
          %{initial: pool_config.initial_workers}
        end

        @impl true
        def handle_event(_event, pool, state) do
          count = ElasticPool.request_count(pool)

          target =
            cond do
              count >= 200 -> 8
              count >= 100 -> 4
              true -> state.initial
            end

          {target, state}
        end
      end

  ## Future Policy Ideas

  ### PID Controllers (Proportional-Integral-Derivative)

  This is the classic control-theory approach. Instead of a simple threshold,
  the system continuously evaluates three signals:

  - Proportional (P) - "How far are we from the target right now?"
    Example: target `50%` utilization, currently `80%`, so scale up immediately.
  - Integral (I) - "How long has the error persisted?"
    Example: "We've been at `60%` for 10 minutes, maybe we need one more worker."
    This helps smooth out small fluctuations.
  - Derivative (D) - "How fast is the error changing?"
    Example: "Utilization just jumped from `20%` to `50%` in one second, we're
    about to hit a wall, scale up now."

  ### Predictive Scaling (ARIMA / LSTM)

  This is time-series forecasting. If the system knows that every Monday at
  `9:00 AM` traffic spikes by `500%`, it does not wait for the spike. It starts
  spinning up workers before the spike arrives.

  - AWS Predictive Scaling uses this idea to pre-warm resources
  - These approaches look for daily, weekly, and seasonal patterns in demand

  ### Multi-Step Cool-Down Curves

  Instead of a single cooldown value, these systems use aggressive step-scaling
  for scale-up and slower, more conservative scale-down behavior.

  - If the queue is `10x` the threshold, do not just start `1` worker; start `5`
  - When shrinking, reduce capacity gradually to avoid flapping between scale-up
    and scale-down decisions
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
