defmodule ElasticPool.Pool do
  @moduledoc """
  A queue-based pool for workers that takes worker resource cost and startup latency
  into account
  """
  use GenServer
  require Logger

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: config.pool)
  end

  def checkout(pool, timeout \\ :infinity) do
    GenServer.call(pool, :checkout, timeout)
  end

  def checkin(pool, worker_pid) do
    GenServer.cast(pool, {:checkin, worker_pid})
  end

  def worker_ready(pool, worker_pid) do
    GenServer.cast(pool, {:checkin, worker_pid})
  end

  # --- Callbacks ---
  @impl true
  def init(config) do
    state = %{
      available: [],
      waiting: :queue.new(),
      monitors: %{}, # pid -> ref
      peak_workers: 0,
      scale_threshold: config.scale_threshold,
      manager: config.manager,
      stats_table: config.stats_table,
      pool_name: config.name
    }

    update_ets(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:checkout, from, state) do
    case state.available do
      [pid | rest] ->
        new_state = %{state | available: rest}
        update_ets(new_state)
        {:reply, {:ok, nil, pid}, new_state}

      [] ->
        new_waiting = :queue.in(from, state.waiting)
        waiting_count = :queue.len(new_waiting)
        new_state = %{state | waiting: new_waiting}
        update_ets(new_state)

        if waiting_count >= state.scale_threshold do
          ElasticPool.ScalingManager.request_scale_up(state.manager)
        end

        {:noreply, new_state}
    end
  end

  @impl true
  def handle_cast({:checkin, pid}, state) do
    if Process.alive?(pid) do
      state = ensure_monitored(state, pid)
      new_peak = max(state.peak_workers, map_size(state.monitors))
      state = %{state | peak_workers: new_peak}

      case :queue.out(state.waiting) do
        {{:value, from}, rest} ->
          GenServer.reply(from, {:ok, nil, pid})
          new_state = %{state | waiting: rest}
          update_ets(new_state)
          {:noreply, new_state}

        {:empty, _} ->
          if pid in state.available do
            {:noreply, state}
          else
            new_state = %{state | available: [pid | state.available]}
            update_ets(new_state)
            {:noreply, new_state}
          end
      end
    else
      new_state = handle_down(state, pid)
      update_ets(new_state)
      {:noreply, new_state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    new_state = handle_down(state, pid)
    update_ets(new_state)
    {:noreply, new_state}
  end

  # --- Private ---

  defp update_ets(state) do
    stats = [
      total_workers: map_size(state.monitors),
      available_workers: length(state.available),
      peak_workers: state.peak_workers,
      waiting_clients: :queue.len(state.waiting)
    ]
    :ets.insert(state.stats_table, stats)
    state
  end

  defp ensure_monitored(state, pid) do
    if Map.has_key?(state.monitors, pid) do
      state
    else
      ref = Process.monitor(pid)
      %{state | monitors: Map.put(state.monitors, pid, ref)}
    end
  end

  defp handle_down(state, pid) do
    new_monitors = Map.delete(state.monitors, pid)
    new_available = Enum.reject(state.available, &(&1 == pid))
    %{state | monitors: new_monitors, available: new_available}
  end
end
