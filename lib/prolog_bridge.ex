defmodule PrologBridge do
  @moduledoc """
  High-level API for the Prolog Bridge.
  """

  def query(query_str, timeout \\ 30_000) do
    case PrologBridge.Pool.checkout(timeout) do
      {:ok, _pool_ref, worker_pid} ->
        try do
          PrologBridge.Worker.query(worker_pid, query_str, timeout)
        after
          PrologBridge.Pool.checkin(worker_pid)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def status do
    PrologBridge.Pool.status()
  end
end
