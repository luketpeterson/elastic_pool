defmodule ElasticPool.Pool do
  @moduledoc """
  Facade for the pool implementation.
  """

  @impl_mod Application.compile_env(:elastic_pool, :pool_implementation, ElasticPool.Pool.Atomic)

  @doc """
  Returns the current pool implementation module.
  """
  @spec implementation() :: module()
  def implementation, do: @impl_mod

  def checkout(pool_name, timeout \\ :infinity) do
    @impl_mod.checkout(pool_name, timeout)
  end

  def checkin(pool_name, worker_pid) do
    @impl_mod.checkin(pool_name, worker_pid)
  end

  def add_worker(pool_name, worker_pid) do
    @impl_mod.add_worker(pool_name, worker_pid)
  end

  @doc false
  def pool_children(config) do
    @impl_mod.pool_children(config)
  end
end
