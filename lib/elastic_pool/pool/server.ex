defmodule ElasticPool.Pool.Server do
  @moduledoc false
  use GenServer
  import ElasticPool.Atomics

  def start_link(config) do
    name = Module.concat(config.name, PoolServer)
    GenServer.start_link(__MODULE__, config, name: name)
  end

  @spec checkout(atom(), timeout()) :: {:ok, nil, pid()} | {:error, :timeout}
  def checkout(pool_name, timeout \\ :infinity) do
    server = Module.concat(pool_name, PoolServer)
    try do
      GenServer.call(server, :checkout, timeout)
    catch
      :exit, {:timeout, _} -> {:error, :timeout}
    end
  end

  @spec checkin(atom(), pid()) :: :ok
  def checkin(pool_name, worker_pid) do
    server = Module.concat(pool_name, PoolServer)
    GenServer.cast(server, {:checkin, worker_pid})
  end

  @spec add_worker(atom(), pid()) :: :ok
  def add_worker(pool_name, worker_pid) do
    checkin(pool_name, worker_pid)
  end

  @doc false
  def setup_pool(_config), do: :ok

  @spec dismiss_workers(atom(), non_neg_integer()) :: :ok
  def dismiss_workers(pool_name, n) do
    server = Module.concat(pool_name, PoolServer)
    GenServer.cast(server, {:dismiss_workers, n})
  end

  @spec worker_exit(atom(), pid()) :: :ok
  def worker_exit(pool_name, pid) do
    server = Module.concat(pool_name, PoolServer)
    GenServer.cast(server, {:worker_exit, pid})
  end

  @doc false
  def pool_children(config), do: [{__MODULE__, config}]

  # --- Callbacks ---

  @impl true
  def init(config) do
    {:ok, %{
      config: config,
      pool_name: config.name,
      atomics: config.atomics,
      manager: config.manager,
      idle_workers: [], # LIFO Stack
      waiting_clients: :queue.new(),
      monitors: %{} # ref -> from
    }}
  end

  @impl true
  def handle_call(:checkout, from, state) do
    # Track request count
    req_idx = :atomics.add_get(state.atomics, request_idx(), 1)
    if rem(req_idx, state.config.sampling_rate) == 0 do
      notify_manager(state.manager, {:checkout_sample, weight: state.config.sampling_rate})
    end

    case state.idle_workers do
      [pid | rest] ->
        if Process.alive?(pid) do
          :atomics.add(state.atomics, score_idx(), -1)
          {:reply, {:ok, nil, pid}, %{state | idle_workers: rest}}
        else
          # Discard dead worker and retry checkout (effectively)
          handle_call(:checkout, from, %{state | idle_workers: rest})
        end

      [] ->
        if :queue.is_empty(state.waiting_clients) do
          notify_manager(state.manager, :saturation_regime)
        end
        :atomics.add(state.atomics, score_idx(), -1)

        # Monitor the client for timeout/exit
        {client_pid, _tag} = from
        ref = Process.monitor(client_pid)

        new_waiting = :queue.in({from, ref}, state.waiting_clients)
        new_monitors = Map.put(state.monitors, ref, from)

        {:noreply, %{state | waiting_clients: new_waiting, monitors: new_monitors}}
    end
  end

  @impl true
  def handle_cast({:checkin, worker_pid}, state) do
    # Track completion count
    comp_idx = :atomics.add_get(state.atomics, completion_idx(), 1)
    if rem(comp_idx, state.config.sampling_rate) == 0 do
      notify_manager(state.manager, {:checkin_sample, weight: state.config.sampling_rate})
    end

    case next_waiting_client(state.waiting_clients, state.monitors) do
      {{:value, from, ref}, new_waiting, new_monitors} ->
        Process.demonitor(ref, [:flush])
        :atomics.add(state.atomics, score_idx(), 1)
        GenServer.reply(from, {:ok, nil, worker_pid})
        {:noreply, %{state | waiting_clients: new_waiting, monitors: new_monitors}}

      {:empty, new_waiting, new_monitors} ->
        if :atomics.add_get(state.atomics, score_idx(), 1) == 1 do
          notify_manager(state.manager, :idle_regime)
        end
        {:noreply, %{state | idle_workers: [worker_pid | state.idle_workers], waiting_clients: new_waiting, monitors: new_monitors}}
    end
  end

  @impl true
  def handle_cast({:dismiss_workers, n}, state) do
    {to_kill, rest} = Enum.split(state.idle_workers, n)

    Enum.each(to_kill, fn pid ->
      :atomics.add(state.atomics, score_idx(), -1)
      Process.exit(pid, :normal)
    end)

    {:noreply, %{state | idle_workers: rest}}
  end

  @impl true
  def handle_cast({:worker_exit, pid}, state) do
    if pid in state.idle_workers do
      :atomics.add(state.atomics, score_idx(), -1)
      {:noreply, %{state | idle_workers: List.delete(state.idle_workers, pid)}}
    else
      # Worker was busy, score already accounted for
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    if Map.has_key?(state.monitors, ref) do
      # Client died or timed out while waiting.
      # We don't remove from the queue immediately to keep it simple,
      # but we MUST increment the score back.
      :atomics.add(state.atomics, score_idx(), 1)
      {:noreply, %{state | monitors: Map.delete(state.monitors, ref)}}
    else
      {:noreply, state}
    end
  end

  # --- Internal ---

  defp next_waiting_client(waiting, monitors) do
    case :queue.out(waiting) do
      {{:value, {from, ref}}, rest} ->
        if Map.has_key?(monitors, ref) do
          {{:value, from, ref}, rest, Map.delete(monitors, ref)}
        else
          # This client already timed out (handled in handle_info)
          next_waiting_client(rest, monitors)
        end
      {:empty, _} ->
        {:empty, waiting, monitors}
    end
  end

  defp notify_manager(manager, event) do
    GenServer.cast(manager, {:policy_event, event})
  end
end
