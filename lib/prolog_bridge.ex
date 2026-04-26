defmodule PrologBridge do
  @moduledoc """
  High-level API for the Prolog Bridge using NimblePool + Worker processes.
  """

  def query(query_str, timeout \\ 30_000) do
    # callback: fn worker_state, resource -> {return, new_worker_state} end
    NimblePool.checkout!(PrologBridge.Pool, :query, fn _worker_state, worker_pid ->
      # Use the worker_pid (the resource) to call the GenServer
      result = PrologBridge.Worker.query(worker_pid, query_str, timeout)
      {result, :ready}
    end, timeout)
  end

  def status do
    %{total_processes: "Managed by NimblePool"}
  end
end
