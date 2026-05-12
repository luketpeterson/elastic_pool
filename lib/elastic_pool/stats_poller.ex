defmodule ElasticPool.StatsPoller do
  @moduledoc false
  # A periodic poller that emits telemetry heartbeats for an ElasticPool.

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    interval = opts[:interval] || 5000

    # Schedule first tick
    schedule_poll(interval)

    {:ok, %{config: config, interval: interval}}
  end

  defmacro get_stat_safe(table, key) do
    quote do
      try do
        :ets.lookup_element(unquote(table), unquote(key), 2)
      rescue
        ArgumentError -> 0
      end
    end
  end

  @impl true
  def handle_info(:poll, state) do
    pool_name = state.config.name
    atomics = state.config.atomics
    import ElasticPool.Atomics

    # Read from atomics (score, active, peak, requests, target)
    score = :atomics.get(atomics, score_idx())
    active = :atomics.get(atomics, active_idx())
    available = if score > 0, do: score, else: 0
    waiting = if score < 0, do: abs(score), else: 0

    measurements = %{
      target_workers: :atomics.get(atomics, target_idx()),
      active_workers: active,
      available_workers: available,
      busy_workers: active - available,
      peak_workers: :atomics.get(atomics, peak_idx()),
      waiting_clients: waiting,
      request_count: :atomics.get(atomics, request_idx())
    }

    # Emit the heartbeat
    :telemetry.execute([:elastic_pool, :pool, :status], measurements, %{pool_name: pool_name})

    schedule_poll(state.interval)
    {:noreply, state}
  end

  defp schedule_poll(interval) do
    Process.send_after(self(), :poll, interval)
  end
end
