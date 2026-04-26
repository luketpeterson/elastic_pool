defmodule PrologBridge.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    pool_size = Application.get_env(:prolog_bridge, :pool_max_workers, 16)
    baseline = Application.get_env(:prolog_bridge, :pool_baseline_workers, 2)

    children = [
      {PrologBridge.WorkerSupervisor, []},
      {PrologBridge.WorkerPool, [max_workers: pool_size]},
      {NimblePool,
       worker: {PrologBridge.Pool, []},
       pool_size: pool_size,
       name: PrologBridge.Pool}
    ]

    opts = [strategy: :one_for_one, name: PrologBridge.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        # Start baseline workers as fast as possible.
        # Because Worker.init/1 is now fast, all will start almost simultaneously
        # and execute their handshakes in parallel.
        1..baseline
        |> Enum.each(fn _ ->
          {:ok, _worker_pid} = PrologBridge.WorkerSupervisor.start_worker()
        end)

        # Wait until the baseline workers are fully initialized and registered in the pool
        wait_for_workers(baseline)

        {:ok, pid}

      error -> error
    end
  end

  defp wait_for_workers(target) do
    status = PrologBridge.status()
    if status.total_workers < target do
      Process.sleep(10)
      wait_for_workers(target)
    else
      :ok
    end
  end
end
