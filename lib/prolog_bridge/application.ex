defmodule PrologBridge.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    pool_size = 16
    baseline = 2

    children = [
      {NimblePool,
       worker: {PrologBridge.Pool, [pool_size: pool_size]},
       pool_size: pool_size,
       name: PrologBridge.Pool}
    ]

    opts = [strategy: :one_for_one, name: PrologBridge.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        # Synchronously warmup the baseline
        1..baseline
        |> Enum.map(fn _ ->
          Task.async(fn ->
            PrologBridge.query("is_valid(1)")
          end)
        end)
        |> Task.await_many(60_000)

        {:ok, pid}

      error -> error
    end
  end
end
