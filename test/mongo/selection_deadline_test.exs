defmodule Mongo.SelectionDeadlineTest do
  use ExUnit.Case, async: true

  alias Mongo.Error
  alias Mongo.Session
  alias Mongo.Topology

  @checkout_timeout 500
  @reply_delay 300
  @selection_error "server selection timeout after #{@checkout_timeout} ms"

  # Replies `:new_connection` to every selection request, as the topology does when a pool starts.
  defmodule NewConnectionTopology do
    use GenServer

    def start_link(delay), do: GenServer.start_link(__MODULE__, delay)

    def init(delay), do: {:ok, delay}

    def handle_call(_request, from, delay) do
      Process.send_after(self(), {:reply, from}, delay)
      {:noreply, delay}
    end

    def handle_info({:reply, from}, delay) do
      GenServer.reply(from, :new_connection)
      {:noreply, delay}
    end
  end

  setup do
    %{topology: start_supervised!({NewConnectionTopology, @reply_delay})}
  end

  test "start_session keeps one deadline across new connection replies", %{topology: topology} do
    assert_selection_error_in_time(fn -> Session.start_session(topology, :write, checkout_timeout: @checkout_timeout) end)
  end

  test "select_server keeps one deadline across new connection replies", %{topology: topology} do
    assert_selection_error_in_time(fn -> Topology.select_server(topology, :write, checkout_timeout: @checkout_timeout) end)
  end

  defp assert_selection_error_in_time(fun) do
    task = Task.async(fn -> :timer.tc(fun) end)

    assert {:ok, {elapsed_us, result}} = Task.yield(task, @checkout_timeout * 4) || Task.shutdown(task, :brutal_kill)
    assert {:error, %Error{message: @selection_error}} = result
    assert div(elapsed_us, 1000) in @checkout_timeout..(@checkout_timeout + 100)
  end
end
