defmodule ElasticPool.StatsPoller do
  @moduledoc """
  A periodic poller that emits telemetry heartbeats for an ElasticPool.

  Add this to your supervision tree to enable periodic status reporting:

      children = [
        {ElasticPool, name: MyPool, ...},
        {ElasticPool.StatsPoller, pool: MyPool, interval: 5000}
      ]
  """
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    pool = Keyword.fetch!(opts, :pool)
    interval = opts[:interval] || 5000

    # Schedule first tick
    schedule_poll(interval)

    {:ok, %{pool: pool, interval: interval}}
  end

  @impl true
  def handle_info(:poll, state) do
    # Read absolute state using high-performance accessors
    measurements = %{
      total_workers: ElasticPool.total_workers(state.pool),
      available_workers: ElasticPool.available_workers(state.pool),
      peak_workers: ElasticPool.peak_workers(state.pool),
      waiting_clients: ElasticPool.waiting_clients(state.pool)
    }

    # Emit the heartbeat
    :telemetry.execute([:elastic_pool, :pool, :status], measurements, %{pool_name: state.pool})

    schedule_poll(state.interval)
    {:noreply, state}
  end

  defp schedule_poll(interval) do
    Process.send_after(self(), :poll, interval)
  end
end
