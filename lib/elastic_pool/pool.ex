defmodule ElasticPool.Pool do
  @moduledoc false
  import ElasticPool.Atomics

  # Max iterations to spin-retry when the atomic counter and ETS are temporarily out of sync.
  # We use a large limit but include a yield to avoid hard-locking the scheduler.
  @max_spin 10_000

  @spec checkout(atom(), timeout()) :: {:ok, reference() | nil, pid()} | {:error, :timeout | :no_workers}
  def checkout(pool_name, timeout \\ :infinity) do
    atomics = :ets.lookup_element(pool_name, :atomics, 2)

    # Track request count and sample for policy
    req_idx = :atomics.add_get(atomics, request_idx(), 1)

    sampling_rate = :ets.lookup_element(pool_name, :sampling_rate, 2)
    if rem(req_idx, sampling_rate) == 0 do
      manager = Module.concat(pool_name, WorkerManager)
      GenServer.cast(manager, {:policy_event, {:checkout_sample, weight: sampling_rate}})
    end

    case :atomics.add_get(atomics, score_idx(), -1) do
      s when s >= 0 ->
        # CLAIMED: Take next worker from FIFO queue.
        target = :atomics.add_get(atomics, worker_pop_idx(), 1)
        table = Module.concat(pool_name, AvailableWorkers)

        case spin_take(table, target, @max_spin) do
          {:ok, pid} ->
            if Process.alive?(pid) do
              {:ok, nil, pid}
            else
              # Worker died. Restore score and retry.
              :atomics.add(atomics, score_idx(), 1)
              checkout(pool_name, timeout)
            end

          :error ->
            # Contention or very late pusher. Restore score and retry.
            :atomics.add(atomics, score_idx(), 1)
            checkout(pool_name, timeout)
        end

      _s ->
        # SATURATED: Register in client FIFO queue and wait.
        target = :atomics.add_get(atomics, client_push_idx(), 1)
        wait_for_worker(pool_name, atomics, target, timeout)
    end
  end

  @spec checkin(atom(), pid()) :: :ok
  def checkin(pool_name, worker_pid) do
    atomics = :ets.lookup_element(pool_name, :atomics, 2)

    # Track completion count and sample for policy
    comp_idx = :atomics.add_get(atomics, completion_idx(), 1)

    sampling_rate = :ets.lookup_element(pool_name, :sampling_rate, 2)
    if rem(comp_idx, sampling_rate) == 0 do
      manager = Module.concat(pool_name, WorkerManager)
      GenServer.cast(manager, {:policy_event, {:checkin_sample, weight: sampling_rate}})
    end

    case :atomics.add_get(atomics, score_idx(), 1) do
      s when s <= 0 ->
        # HANDOFF: Satisfy next client from FIFO queue.
        target = :atomics.add_get(atomics, client_pop_idx(), 1)
        table = Module.concat(pool_name, WaitingClients)

        case spin_take(table, target, @max_spin) do
          {:ok, {ref, client_pid}} ->
            send(client_pid, {:elastic_pool, :worker, ref, worker_pid})
            :ok

          :error ->
            # Client timed out or very late pusher.
            # Re-checkin the worker to trigger next handoff or go idle.
            checkin(pool_name, worker_pid)
        end

      _s ->
        # IDLE: Put worker into available FIFO queue.
        target = :atomics.add_get(atomics, worker_push_idx(), 1)
        table = Module.concat(pool_name, AvailableWorkers)
        :ets.insert(table, {target, worker_pid})
        :ok
    end
  end

  @spec add_worker(atom(), pid()) :: :ok
  def add_worker(pool_name, worker_pid) do
    checkin(pool_name, worker_pid)
  end

  # --- Internal Logic ---

  defp wait_for_worker(pool_name, atomics, index, timeout) do
    table = Module.concat(pool_name, WaitingClients)
    ref = make_ref()
    :ets.insert(table, {index, {ref, self()}})

    receive do
      {:elastic_pool, :worker, ^ref, worker_pid} ->
        {:ok, nil, worker_pid}
    after
      timeout ->
        case :ets.take(table, index) do
          [{^index, {^ref, _}}] ->
            :atomics.add(atomics, score_idx(), 1)
            {:error, :timeout}

          [] ->
            receive do
              {:elastic_pool, :worker, ^ref, worker_pid} ->
                {:ok, nil, worker_pid}
            end
        end
    end
  end

  defp spin_take(_table, _target, 0), do: :error
  defp spin_take(table, target, limit) do
    case :ets.take(table, target) do
      [{^target, item}] -> {:ok, item}
      [] ->
        if rem(limit, 100) == 0, do: :erlang.yield()
        spin_take(table, target, limit - 1)
    end
  end
end
