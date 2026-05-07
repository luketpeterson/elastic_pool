defmodule ElasticPool.Worker do
  @moduledoc """
  Public behaviour and `use` macro for ElasticPool workers.

  To implement a worker, use this module:

      defmodule MyWorker do
        use ElasticPool.Worker

        @impl true
        def handle_work(request, _from, state) do
          {:reply, :ok, state}
        end
      end
  """

  @callback init(args :: term()) :: state :: term() | {:stop, reason :: term()}
  @callback handle_work(request :: term(), from :: term(), state :: term()) ::
              {:reply, reply :: term(), new_state :: term()}
              | {:noreply, new_state :: term()}
  @callback terminate(reason :: term(), state :: term()) :: term()

  @optional_callbacks init: 1, terminate: 2

  defmacro __using__(_opts) do
    quote do
      @behaviour ElasticPool.Worker

      @impl true
      def init(args), do: args

      @impl true
      def terminate(_reason, _state), do: :ok

      defoverridable init: 1, terminate: 2
    end
  end
end
