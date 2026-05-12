defmodule ElasticPool.Pool do
  @moduledoc false
  import ElasticPool.Atomics

  # Max iterations to spin-retry when the atomic counter and ETS are temporarily out of sync
  @max_spin 100

  @spec checkout(atom(), timeout()) :: {:ok, reference() | nil, pid()} | {:error, :timeout | :no_workers}

  def checkout(pool_name, timeout \\ :infinity) do
    atomics = get_atomics(pool_name)
    :atomics.add(atomics, request_idx(), 1)

    case :atomics.add_get(atomics, score_idx(), -1) do
      s when s >= 0 ->
        # Fast path: An idle worker is available in ETS
      case take_worker_spin(pool_name, @max_spin) do
          {:ok, pid} ->
            {:ok, nil, pid}

          :error ->
            # Atomic counter and ETS are out of sync.
            # Correct the score and attempt to wait.
            :atomics.add(atomics, score_idx(), 1)
            wait_for_worker(pool_name, atomics, timeout)
        end

      _s ->
        # Slow path: Pool is empty, must wait for a checkin
        wait_for_worker(pool_name, atomics, timeout)
    end
  end

  @spec checkin(atom(), pid()) :: :ok
  def checkin(pool_name, worker_pid) do
    atomics = get_atomics(pool_name)

    case :atomics.add_get(atomics, score_idx(), 1) do
      s when s <= 0 ->
        # Handoff path: A client is waiting in ETS
        case take_client_spin(pool_name, @max_spin) do
          {:ok, {ref, client_pid}} ->
            send(client_pid, {:elastic_pool, :worker, ref, worker_pid})
            :ok

          :error ->
            # Atomic counter and ETS are out of sync. Treat worker as idle.
            put_worker_idle(pool_name, worker_pid)
            :ok
        end

      _s ->
        # Idle path: No one is waiting
        put_worker_idle(pool_name, worker_pid)
        # Signal the manager that a checkin occurred (hint for scale-down)
        manager = Module.concat(pool_name, WorkerManager)
        GenServer.cast(manager, {:policy_event, :checkin})
        :ok
    end
  end

  @spec add_worker(atom(), pid()) :: :ok
  def add_worker(pool_name, worker_pid) do
    # New workers use the same checkin logic to enter rotation
    checkin(pool_name, worker_pid)
  end

  # --- Internal Logic ---

  defp get_atomics(pool_name) do
    :ets.lookup_element(pool_name, :atomics, 2)
  end

  defp take_worker_spin(_pool_name, 0), do: :error

  defp take_worker_spin(pool_name, limit) do
    table = Module.concat(pool_name, AvailableWorkers)

    case :ets.first(table) do
      :"$end_of_table" ->
        take_worker_spin(pool_name, limit - 1)

      pid when is_pid(pid) ->
        case :ets.take(table, pid) do
          [{^pid}] ->
            if Process.alive?(pid) do
              {:ok, pid}
            else
              # Worker died between ETS insertion and take.
              atomics = get_atomics(pool_name)
              :atomics.add(atomics, score_idx(), 1)
              take_worker_spin(pool_name, limit - 1)
            end

          [] ->
            # Race: Someone else took it
            take_worker_spin(pool_name, limit - 1)
        end
    end
  end

  defp take_client_spin(_pool_name, 0), do: :error

  defp take_client_spin(pool_name, limit) do
    table = Module.concat(pool_name, WaitingClients)

    case :ets.first(table) do
      :"$end_of_table" ->
        take_client_spin(pool_name, limit - 1)

      ref ->
        case :ets.take(table, ref) do
          [{^ref, client_pid}] -> {:ok, {ref, client_pid}}
          [] -> take_client_spin(pool_name, limit - 1)
        end
    end
  end

  defp wait_for_worker(pool_name, atomics, timeout) do
    table = Module.concat(pool_name, WaitingClients)
    ref = make_ref()
    :ets.insert(table, {ref, self()})

    # Signal the manager that a checkout failed (hint for scale-up)
    manager = Module.concat(pool_name, WorkerManager)
    GenServer.cast(manager, {:policy_event, :checkout_failed})

    receive do
      {:elastic_pool, :worker, ^ref, worker_pid} ->
        {:ok, nil, worker_pid}
    after
      timeout ->
        # Cleanup on timeout
        :ets.delete(table, ref)

        # Atomic Correction: We "undo" our reservation.
        # This might trigger a "Ghost Client" for a concurrent checkin,
        # but the checkin logic handles it by putting the worker back to idle.
        :atomics.add(atomics, score_idx(), 1)
        {:error, :timeout}
    end
  end

  defp put_worker_idle(pool_name, worker_pid) do
    table = Module.concat(pool_name, AvailableWorkers)
    :ets.insert(table, {worker_pid})
  end
end
