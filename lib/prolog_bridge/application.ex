defmodule PrologBridge.Application do
  use Application

  @impl true
  def start(_type, _args) do
    pool_config = [
      name: {:local, :prolog_pool},
      worker_module: PrologBridge.Worker,
      # The number of worker swipl processes to initially start
      size: 2,
      # The maximum number of additional swipl processes to start as load requires
      max_overflow: 14
    ]

    children = [
      :poolboy.child_spec(:prolog_pool, pool_config, [])
    ]

    opts = [strategy: :one_for_one, name: PrologBridge.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        # Initialization Phase:
        # We ensure the baseline workers are hot by running a dummy query for each.
        # This blocks Application.start until the KB is loaded in the baseline.
        1..pool_config[:size]
        |> Enum.map(fn _ -> Task.async(fn -> PrologBridge.query("is_valid(1)") end) end)
        |> Task.await_many(60_000)

        {:ok, pid}

      error -> error
    end
  end
end
