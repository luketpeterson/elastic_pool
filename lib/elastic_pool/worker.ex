defmodule ElasticPool.Worker do
  use GenServer
  require Logger

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  def work(pid, duration_ms, timeout) do
    GenServer.call(pid, {:work, duration_ms}, timeout)
  end

  @impl true
  def init(_args) do
    # Signal that we are ready
    # In a real scenario, this might wait for some initialization
    # Here we just notify the managers
    send(self(), :notify_ready)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:notify_ready, state) do
    ElasticPool.ScalingManager.worker_ready()
    ElasticPool.Pool.worker_ready(self())
    {:noreply, state}
  end

  @impl true
  def handle_call({:work, duration_ms}, _from, state) do
    Process.sleep(duration_ms)
    {:reply, :ok, state}
  end
end
