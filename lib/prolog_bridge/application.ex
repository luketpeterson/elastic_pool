defmodule PrologBridge.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    # Centralized configuration
    config = %{
      max_workers: Application.get_env(:prolog_bridge, :pool_max_workers, 8),
      baseline_workers: Application.get_env(:prolog_bridge, :pool_baseline_workers, 2),
      cooldown_ms: Application.get_env(:prolog_bridge, :pool_cooldown_ms, 500),
      scale_threshold: Application.get_env(:prolog_bridge, :pool_scale_threshold, 100)
    }


    children = [
      {PrologBridge.WorkerSupervisor, []},
      {PrologBridge.ScalingManager, config},
      {PrologBridge.Pool, []}
    ]

    opts = [strategy: :one_for_one, name: PrologBridge.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        # Start baseline workers
        1..config.baseline_workers
        |> Enum.each(fn _ ->
          {:ok, _worker_pid} = PrologBridge.WorkerSupervisor.start_worker()
        end)

        wait_for_workers(config.baseline_workers)

        {:ok, pid}

      error -> error
    end
  end

  defp wait_for_workers(target) do
    status = PrologBridge.status()
    if status.total_ready_workers < target do
      Process.sleep(10)
      wait_for_workers(target)
    else
      :ok
    end
  end
end
