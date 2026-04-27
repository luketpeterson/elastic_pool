defmodule PrologBridge.Worker do
  use GenServer
  require Logger

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  def query(pid, query_str, timeout) do
    GenServer.call(pid, {:query, query_str}, timeout)
  end

  @impl true
  def init(args) do
    # Trap exit to ensure terminate/2 is called
    Process.flag(:trap_exit, true)

    executable = "swipl"
    server_path = Application.app_dir(:prolog_bridge, "priv/prolog/server.pl")
    args_list = ["-q", "-s", server_path, "-g", "main"]
    kb_file = args[:kb_file]

    port = Port.open({:spawn_executable, System.find_executable(executable)}, [
      :binary, :exit_status, args: args_list,
      env: [{~c"KB_FILE", String.to_charlist(kb_file)}]
    ])

    # Synchronously wait for the handshake signal from the Prolog process.
    receive do
      {^port, {:data, _data}} ->
        PrologBridge.WorkerPool.worker_ready(self())
        {:ok, %{args: args, port: port, buffer: "", caller: nil}}
      {^port, {:exit_status, status}} ->
        {:stop, {:prolog_start_failed, status}}
    end
  end

  @impl true
  def handle_call({:query, query_str}, from, state) do
    payload = Jason.encode!(%{query: query_str}) <> "\n"
    Port.command(state.port, payload)
    {:noreply, %{state | caller: from}}
  end

  @impl true
  def handle_info({port, {:data, data}}, %{port: port, caller: caller, buffer: buffer} = state) do
    new_buffer = buffer <> data
    if String.contains?(new_buffer, "\n") do
      [line | _] = String.split(new_buffer, "\n")
      if caller, do: GenServer.reply(caller, {:ok, Jason.decode!(line)})
      {:noreply, %{state | caller: nil, buffer: ""}}
    else
      {:noreply, %{state | buffer: new_buffer}}
    end
  end

  @impl true
  def handle_info({_port, {:exit_status, _}}, state), do: {:stop, :prolog_exit, state}

  @impl true
  def terminate(_reason, state) do
    if state.port do
      # 1. Try to send a clean halt command
      try do
        Port.command(state.port, Jason.encode!(%{command: "halt"}) <> "\n")
      rescue
        _ -> :ok
      end

      # 2. Wait for the OS process to actually exit
      receive do
        {_port, {:exit_status, _status}} ->
          :ok
      after
        5_000 ->
          # Force close if it takes too long
          Logger.warning("[Worker] Prolog process failed to exit gracefully within 5s. Force closing port.")
          Port.close(state.port)
          :ok
      end
    end
    :ok
  end
end
