defmodule Mongo.TransactionReselectTest do
  use ExUnit.Case, async: true

  alias Mongo.Session
  alias Mongo.Session.ServerSession

  # Starts real sessions on a fake connection and answers each write selection after `select_delay_ms`.
  defmodule FakeTopology do
    use GenServer

    alias Mongo.Session

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    def init(opts), do: {:ok, Map.new(opts)}

    def handle_call({:checkout_session, :write, opts, _deadline, _ref}, _from, state) do
      {:reply, Session.start_link(self(), :old_conn, "old:27017", ServerSession.new(), 0, opts), state}
    end

    def handle_call({:select_server, :write, opts}, from, state) do
      send(state.test_pid, {:select_server, opts})
      Process.send_after(self(), {:reply, from}, state.select_delay_ms)
      {:noreply, state}
    end

    def handle_cast({:checkin_session, _server_session}, state), do: {:noreply, state}

    def handle_cast({:server_description, description}, state) do
      send(state.test_pid, {:marked_unknown, description.address})
      {:noreply, state}
    end

    def handle_info({:reply, from}, state) do
      GenServer.reply(from, {:ok, :new_conn, "new:27017", 21})
      {:noreply, state}
    end
  end

  @step_down Mongo.Error.exception(%{"code" => 10_107, "errmsg" => "not primary", "errorLabels" => ["TransientTransactionError"]})

  test "a float transaction_retry_timeout_s gives an integer selection timeout" do
    topology = start_supervised!({FakeTopology, test_pid: self(), select_delay_ms: 0})

    assert {:ok, :done} = Mongo.transaction(topology, step_down_once(), transaction_retry_timeout_s: 1.5, checkout_timeout: 5_000)
    assert_received {:select_server, opts}
    assert is_integer(opts[:checkout_timeout])
  end

  test "a rerun after a step-down binds the session to the new server" do
    topology = start_supervised!({FakeTopology, test_pid: self(), select_delay_ms: 0})
    test_pid = self()

    on_rerun = fn ->
      session = Process.get(:session)
      send(test_pid, {:bound, Session.connection(session), Session.wire_version(session)})
      Session.mark_server_unknown(session)
    end

    assert {:ok, :done} = Mongo.transaction(topology, step_down_once(on_rerun), transaction_retry_timeout_s: 1)
    assert_received {:bound, :new_conn, 21}
    assert_receive {:marked_unknown, "new:27017"}
  end

  test "a rerun waits for a new primary longer than checkout_timeout while the window lasts" do
    topology = start_supervised!({FakeTopology, test_pid: self(), select_delay_ms: 700})

    assert {:ok, :done} = Mongo.transaction(topology, step_down_once(), transaction_retry_timeout_s: 2, checkout_timeout: 500)
  end

  test "the wait for a new primary ends with the rerun window" do
    topology = start_supervised!({FakeTopology, test_pid: self(), select_delay_ms: 5_000})

    {elapsed_us, result} = :timer.tc(fn -> Mongo.transaction(topology, step_down_once(), transaction_retry_timeout_s: 1, checkout_timeout: 5_000) end)

    assert {:error, %Mongo.Error{code: 10_107}} = result
    assert div(elapsed_us, 1000) in 900..1_200
  end

  defp step_down_once(on_rerun \\ fn -> :ok end) do
    runs = :counters.new(1, [])

    fn ->
      :counters.add(runs, 1, 1)

      if :counters.get(runs, 1) == 1 do
        {:error, @step_down}
      else
        on_rerun.()
        {:ok, :done}
      end
    end
  end
end
