defmodule ElasticPool.Policies.Threshold do
  @moduledoc """
  A scaling policy based on fixed thresholds and target counts.
  """
  @behaviour ElasticPool.ScalingPolicy

  @impl true
  def init(%{policy_opts: opts, pool_config: pool}) do
    %{
      threshold: opts[:scale_threshold] || 10,
      max_workers: pool.max_workers,
      baseline_workers: pool.baseline_workers
    }
  end

  @impl true
  def handle_event(event, pool, state) do
    total = ElasticPool.total_workers(pool)
    waiting = ElasticPool.waiting_clients(pool)

    target =
      case event do
        :checkout_failed ->
          # Calculate how many extra 'threshold-sized' blocks we need
          extra = Float.ceil(waiting / state.threshold) |> trunc()
          min(state.max_workers, total + extra)

        :checkin ->
          # If no one is waiting, we can eventually drift to baseline
          if waiting == 0, do: state.baseline_workers, else: total

        _ ->
          total
      end

    {max(target, state.baseline_workers), state}
  end
end
