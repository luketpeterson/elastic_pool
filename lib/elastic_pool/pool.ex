defmodule ElasticPool.Pool do
  @moduledoc false
  import ElasticPool.Atomics

  @spec checkout(atom(), timeout()) :: {:ok, reference() | nil, pid()} | {:error, :timeout | :no_workers}
  def checkout(pool_name, _timeout \\ :infinity) do
    atomics = :ets.lookup_element(pool_name, :atomics, 2)
    tables = :ets.lookup_element(pool_name, :tables, 2)
    :atomics.add(atomics, request_idx(), 1)

    shard = :erlang.phash2(self(), num_shards())

    case try_shard(shard, atomics, tables.available) do
      {:ok, pid} -> {:ok, nil, pid}
      :empty ->
        other = rem(shard + 1, num_shards())
        case try_shard(other, atomics, tables.available) do
          {:ok, pid} -> {:ok, nil, pid}
          :empty -> {:error, :no_workers}
        end
    end
  end

  defp try_shard(shard, atomics, available_tables) do
    idx = score_idx(shard + 1)
    case :atomics.add_get(atomics, idx, -1) do
      s when s >= 0 ->
        table = elem(available_tables, shard)
        case :ets.match_object(table, :"$1", 1) do
          {[{pid}], _} ->
            if :ets.delete_object(table, {pid}) do
              {:ok, pid}
            else
              # Race: retry once
              try_shard(shard, atomics, available_tables)
            end
          _ ->
            # Serious out-of-sync. Repair.
            :atomics.add(atomics, idx, 1)
            :empty
        end

      _s ->
        :atomics.add(atomics, idx, 1)
        :empty
    end
  end

  @spec checkin(atom(), pid()) :: :ok
  def checkin(pool_name, worker_pid) do
    atomics = :ets.lookup_element(pool_name, :atomics, 2)
    tables = :ets.lookup_element(pool_name, :tables, 2)
    :atomics.add(atomics, completion_idx(), 1)

    shard = :erlang.phash2(worker_pid, num_shards())
    table = elem(tables.available, shard)

    :ets.insert(table, {worker_pid})
    :atomics.add(atomics, score_idx(shard + 1), 1)
    :ok
  end

  @spec add_worker(atom(), pid()) :: :ok
  def add_worker(pool_name, worker_pid) do
    checkin(pool_name, worker_pid)
  end
end
