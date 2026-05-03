defmodule ElasticPool.Policies.Threshold do
  @moduledoc """
  A scaling policy based on fixed thresholds and cooldowns.
  """
  @behaviour ElasticPool.ScalingPolicy

  @impl true
  def init(%{policy_opts: opts, pool_config: pool}) do
    %{
      threshold: opts[:scale_threshold] || 10,
      cooldown_ms: opts[:cooldown_ms] || 500,
      max_workers: pool.max_workers,
      last_scale_time: 0
    }
  end

  @impl true
  def handle_stats(stats, state) do
    now = System.monotonic_time(:millisecond)
    cooldown_passed = (now - state.last_scale_time) > state.cooldown_ms

    if cooldown_passed and 
       stats.total_workers < state.max_workers and 
       stats.waiting_clients >= state.threshold do
      {:scale_up, 1, %{state | last_scale_time: now}}
    else
      {:none, state}
    end
  end
end
