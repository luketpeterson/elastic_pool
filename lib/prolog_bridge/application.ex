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
        # Synchronously warmup the baseline workers
        # We start them via WorkerSupervisor and check them into WorkerPool
        1..baseline
        |> Enum.each(fn _ ->
          {:ok, worker_pid} = PrologBridge.WorkerSupervisor.start_worker()
          PrologBridge.WorkerPool.checkin_worker(worker_pid)
        end)

        {:ok, pid}

      error -> error
    end
  end
end
