defmodule PrologBridge.Worker do
  use GenServer
  require Logger

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  def query(pid, query_str, timeout) do
    GenServer.call(pid, {:query, query_str}, timeout)
  end

  @impl true
  def init(args) do
    # caller: nil must be present to avoid KeyError in handle_call
    {:ok, %{args: args, port: nil, buffer: "", caller: nil}, {:continue, :handshake}}
  end

  @impl true
  def handle_continue(:handshake, state) do
    executable = "swipl"
    server_path = Application.app_dir(:prolog_bridge, "priv/prolog/server.pl")
    args = ["-q", "-s", server_path, "-g", "main"]
    kb_file = state.args[:kb_file]

    port = Port.open({:spawn_executable, System.find_executable(executable)}, [
      :binary, :exit_status, args: args,
      env: [{~c"KB_FILE", String.to_charlist(kb_file)}]
    ])

    receive do
      {^port, {:data, _data}} ->
        {:noreply, %{state | port: port}}
    after
      60_000 -> {:stop, :timeout, state}
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
end
