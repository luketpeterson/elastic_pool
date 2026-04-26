defmodule PrologBridge.Application do
  use Application

  @impl true
  def start(_type, _args) do
    pool_config = [
      name: {:local, :prolog_pool},
      worker_module: PrologBridge.Worker,
      size: 2,
      max_overflow: 14
    ]

    children = [:poolboy.child_spec(:prolog_pool, pool_config, [])]
    Supervisor.start_link(children, strategy: :one_for_one, name: PrologBridge.Supervisor)
  end
end
