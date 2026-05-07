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

  @type worker_state :: term()
  @type worker_reply ::
          {:reply, reply :: term(), new_state :: worker_state()}
          | {:noreply, new_state :: worker_state()}

  @callback init(args :: term()) :: worker_state() | {:stop, reason :: term()}
  @callback handle_work(request :: term(), from :: GenServer.from(), state :: worker_state()) ::
              worker_reply()
  @callback terminate(reason :: term(), state :: worker_state()) :: term()

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
