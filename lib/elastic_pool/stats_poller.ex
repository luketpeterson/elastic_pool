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
    stats_table = state.config.stats_table
    pool_name = state.config.name

    # Read absolute state using high-performance accessors
    active = get_stat_safe(stats_table, :active_workers)
    available = get_stat_safe(stats_table, :available_workers)

    measurements = %{
      target_workers: get_stat_safe(stats_table, :target_workers),
      active_workers: active,
      available_workers: available,
      busy_workers: active - available,
      peak_workers: get_stat_safe(stats_table, :peak_workers),
      waiting_clients: get_stat_safe(stats_table, :waiting_clients),
      request_count: get_stat_safe(stats_table, :request_count)
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
