defmodule ElasticPool.Policies.ErlangC do
  @moduledoc """
  A predictive scaling policy based on Little's Law and
  [Erlang-C](https://en.wikipedia.org/wiki/Erlang_(unit)#Erlang_C_formula)
  queueing theory.

  This policy estimates arrival rate from checkout activity and service time from
  live completions. It then computes the smallest worker count that should keep
  expected queue wait under a target budget.

  This allows the pool to scale from observed demand and task speed rather than
  waiting for a large queue to accumulate first.

  ## Options

  - `:target_wait_ms` - target average queue wait budget. Defaults to `50`
  - `:min_workers` - lower bound for the target. Defaults to the pool's
    `:initial_workers`
  - `:bootstrap_service_time_ms` - initial service time estimate used before
    enough live completion data is available. Defaults to `100`
  - `:smoothing` - EWMA smoothing factor for rate and service-time estimates.
    Defaults to `0.2`
  - `:max_search_workers` - search cap used only when the pool's `:max_workers`
    is `:infinity`. Defaults to `64`
  """

  @behaviour ElasticPool.ScalingPolicy
  require ElasticPool

  @type state :: %{
          target_wait_ms: pos_integer(),
          min_workers: pos_integer(),
          bootstrap_service_time_ms: pos_integer(),
          smoothing: float(),
          max_workers: pos_integer() | :infinity,
          max_search_workers: pos_integer(),
          arrival_rate: float() | nil,
          completion_rate: float() | nil,
          service_time_ms: float(),
          last_arrival_at_ms: integer() | nil,
          last_completion_at_ms: integer() | nil
        }

  @impl true
  @spec init(%{policy_opts: keyword(), pool_config: map()}) :: state()
  def init(%{policy_opts: opts, pool_config: pool}) do
    bootstrap_service_time_ms = opts[:bootstrap_service_time_ms] || 100

    %{
      target_wait_ms: opts[:target_wait_ms] || 50,
      min_workers: opts[:min_workers] || pool.initial_workers,
      bootstrap_service_time_ms: bootstrap_service_time_ms,
      smoothing: opts[:smoothing] || 0.2,
      max_workers: pool.max_workers,
      max_search_workers: opts[:max_search_workers] || 64,
      arrival_rate: opts[:bootstrap_arrival_rate] || 0.0,
      completion_rate: nil,
      service_time_ms: bootstrap_service_time_ms * 1.0,
      last_arrival_at_ms: nil,
      last_completion_at_ms: nil
    }
  end

  @impl true
  @spec handle_event(
          ElasticPool.ScalingPolicy.event(),
          ElasticPool.ScalingPolicy.pool_name(),
          state()
        ) :: {ElasticPool.ScalingPolicy.target_decision(), state()}
  def handle_event(event, pool, state) do
    now_ms = System.monotonic_time(:millisecond)
    state = update_estimates(event, pool, now_ms, state)

    # Recalculate target on every event (Sample, Regime Change, Lifecycle, Periodic)
    # This ensures the policy "takes stock" of the latest data and live pool stats.
    target = desired_target(pool, state)
    {target, state}
  end

  defp update_estimates(event, pool, now_ms, state) do
    case event do
      {:checkout_sample, [weight: w]} -> observe_arrival(now_ms, w, state)
      {:checkin_sample, [weight: w]} -> observe_completion(now_ms, w, pool, state)
      _ -> state
    end
  end

  defp observe_arrival(now_ms, weight, state) do
    %{
      state
      | last_arrival_at_ms: now_ms,
        arrival_rate:
          ewma_rate(now_ms, state.last_arrival_at_ms, weight, state.arrival_rate, state.smoothing)
    }
  end

  defp observe_completion(now_ms, weight, pool, state) do
    completion_rate =
      ewma_rate(now_ms, state.last_completion_at_ms, weight, state.completion_rate, state.smoothing)

    busy_workers = max(1, busy_workers(pool))

    service_time_ms =
      if completion_rate do
        # Completion Rate is jobs/sec. Service Time is sec/job.
        # We assume completions are distributed across busy workers.
        observed_service_time_ms = busy_workers * 1_000.0 / completion_rate
        ewma(state.service_time_ms, observed_service_time_ms, state.smoothing)
      else
        state.service_time_ms
      end

    %{
      state
      | completion_rate: completion_rate,
        service_time_ms: service_time_ms,
        last_completion_at_ms: now_ms
    }
  end

  defp desired_target(pool, state) do
    current_target = ElasticPool.target_workers(pool)
    arrival_rate = state.arrival_rate || 0.0
    service_time_ms = max(state.service_time_ms, 1.0)

    if arrival_rate <= 0.0 do
      maybe_target(state.min_workers, current_target)
    else
      upper = search_upper_bound(current_target, arrival_rate, service_time_ms, state)

      target =
        erlang_c_target(
          arrival_rate,
          service_time_ms,
          state.target_wait_ms,
          state.min_workers,
          upper
        )

      maybe_target(target, current_target)
    end
  end

  defp maybe_target(target, current_target) when target == current_target, do: :no_change
  defp maybe_target(target, _current_target), do: target

  defp ewma_rate(_now_ms, nil, _weight, previous, _smoothing), do: previous

  defp ewma_rate(now_ms, last_ms, weight, previous, smoothing) do
    delta_ms = max(now_ms - last_ms, 1)
    instant_rate = (weight * 1_000.0) / delta_ms

    if previous do
      ewma(previous, instant_rate, smoothing)
    else
      instant_rate
    end
  end

  defp ewma(previous, current, smoothing) do
    smoothing * current + (1.0 - smoothing) * previous
  end

  defp busy_workers(pool) do
    ElasticPool.active_workers(pool) - ElasticPool.available_workers(pool)
  end

  defp search_upper_bound(current_target, arrival_rate, service_time_ms, state) do
    offered_load = Float.ceil(arrival_rate * service_time_ms / 1_000.0) |> trunc()
    dynamic_upper = max(current_target + 8, offered_load + 8)

    case state.max_workers do
      :infinity -> max(state.min_workers, min(dynamic_upper, state.max_search_workers))
      max_workers -> max(state.min_workers, min(dynamic_upper, max_workers))
    end
  end

  defp erlang_c_target(arrival_rate, service_time_ms, target_wait_ms, min_workers, upper) do
    service_rate = 1_000.0 / service_time_ms
    offered_load = arrival_rate / service_rate

    Enum.find(min_workers..upper, upper, fn workers ->
      expected_wait_ms(arrival_rate, service_rate, offered_load, workers) <= target_wait_ms
    end)
  end

  defp expected_wait_ms(_arrival_rate, _service_rate, offered_load, workers)
       when offered_load <= 0.0 or workers <= 0 do
    0.0
  end

  defp expected_wait_ms(arrival_rate, service_rate, offered_load, workers) do
    capacity = workers * service_rate

    if arrival_rate >= capacity do
      :infinity
    else
      wait_probability = erlang_c_wait_probability(offered_load, workers)
      expected_wait_seconds = wait_probability / (capacity - arrival_rate)
      expected_wait_seconds * 1_000.0
    end
  end

  defp erlang_c_wait_probability(offered_load, workers) do
    utilization = offered_load / workers

    if utilization >= 1.0 do
      1.0
    else
      {sum, last_term} =
        if workers == 1 do
          {1.0, 1.0}
        else
          Enum.reduce(1..(workers - 1), {1.0, 1.0}, fn n, {acc, term} ->
            next_term = term * offered_load / n
            {acc + next_term, next_term}
          end)
        end

      term_c = last_term * offered_load / workers
      tail = term_c / (1.0 - utilization)
      p0 = 1.0 / (sum + tail)
      tail * p0
    end
  end
end
