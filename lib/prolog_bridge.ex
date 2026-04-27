defmodule PrologBridge do
  @moduledoc """
  High-level API for the Prolog Bridge.
  """

  def query(query_str, timeout \\ 30_000) do
    worker_pid = PrologBridge.WorkerPool.checkout_worker(timeout)
    try do
      PrologBridge.Worker.query(worker_pid, query_str, timeout)
    after
      PrologBridge.WorkerPool.checkin_worker(worker_pid)
    end
  end

  def status do
    PrologBridge.WorkerPool.status()
  end
end
