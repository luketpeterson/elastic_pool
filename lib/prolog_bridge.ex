defmodule PrologBridge do
  @moduledoc """
  High-level API for the Prolog Bridge using NimblePool + Worker processes.
  """

  def query(query_str, timeout \\ 30_000) do
    NimblePool.checkout!(PrologBridge.Pool, :query, fn _worker_state, _slot ->
      # 1. Get a real worker from the WorkerPool. 
      # This blocks if no worker is ready and triggers scaling.
      worker_pid = PrologBridge.WorkerPool.checkout_worker(timeout)

      try do
        # 2. Perform the query
        PrologBridge.Worker.query(worker_pid, query_str, timeout)
      after
        # 3. Always return the worker to the pool
        PrologBridge.WorkerPool.checkin_worker(worker_pid)
      end
    end, timeout)
  end

  def status do
    PrologBridge.WorkerPool.status()
  end
end
