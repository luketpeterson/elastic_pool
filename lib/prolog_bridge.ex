defmodule PrologBridge do
  def query(query_str, timeout \\ 30000) do
    :poolboy.transaction(:prolog_pool, fn pid ->
      GenServer.call(pid, {:query, query_str}, timeout)
    end, timeout)
  end

  def status do
    {state, ready, _overflow, busy} = :poolboy.status(:prolog_pool)
    %{
      state: state,
      ready_workers: ready,
      busy_workers: busy,
      total_processes: ready + busy
    }
  end
end
