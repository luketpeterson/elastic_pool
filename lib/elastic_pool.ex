defmodule ElasticPool do
  @moduledoc """
  High-level API for the Elastic Pool.
  """

  def work(duration_ms, timeout \\ 30_000) do
    case ElasticPool.Pool.checkout(timeout) do
      {:ok, _pool_ref, worker_pid} ->
        try do
          ElasticPool.Worker.work(worker_pid, duration_ms, timeout)
        after
          ElasticPool.Pool.checkin(worker_pid)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def status do
    ElasticPool.Pool.status()
  end
end
