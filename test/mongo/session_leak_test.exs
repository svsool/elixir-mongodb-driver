defmodule Mongo.SessionLeakTest do
  use ExUnit.Case, async: false

  alias Mongo.Session

  @runs 20
  @coll "session_leak_test"

  setup do
    top = start_supervised!({Mongo, database: "mongodb_test", seeds: ["127.0.0.1:27017"], name: :session_leak_test})
    {:ok, _} = Mongo.update_one(top, @coll, %{_id: "fence"}, %{"$set": %{v: 0}}, upsert: true, w: :majority)
    on_exit(fn -> fail_point_off() end)
    [top: top, before: session_pids(top)]
  end

  @tag :rs_required
  test "transactions end their session on every exit path", %{top: top, before: before} do
    for _ <- 1..@runs do
      assert {:ok, :done} = Mongo.transaction(top, fn -> insert(top) end)

      assert {:error, :rejected} = Mongo.transaction(top, fn -> {:error, :rejected} end)

      assert {:error, %RuntimeError{}} = Mongo.transaction(top, fn -> raise "boom" end)

      fail_point(top, "insert", errorCode: 112, errorLabels: ["TransientTransactionError"])
      assert {:ok, :done} = Mongo.transaction(top, fn -> insert(top) end, transaction_retry_timeout_s: 5)

      fail_point(top, "commitTransaction", errorCode: 6, errorLabels: ["UnknownTransactionCommitResult"])
      assert {:error, %Mongo.Error{code: 6}} = Mongo.transaction(top, fn -> insert(top) end, transaction_retry_timeout_s: 0)

      {:ok, session} = Session.start_session(top, :write, [])
      assert :ok = Session.end_session(top, session)
    end

    assert_sessions_ended(top, before)
    assert {:monitors, []} = Process.info(self(), :monitors)
    assert {:message_queue_len, 0} = Process.info(self(), :message_queue_len)
  end

  defp insert(top) do
    with {:ok, _} <- Mongo.insert_one(top, @coll, %{v: 1}), do: {:ok, :done}
  end

  defp fail_point(top, command, data) do
    {:ok, _} = Mongo.admin_command(top, configureFailPoint: "failCommand", mode: [times: 1], data: [failCommands: [command]] ++ data)
  end

  defp fail_point_off do
    {:ok, admin} = Mongo.start_link(database: "admin", seeds: ["127.0.0.1:27017"])
    Mongo.admin_command(admin, configureFailPoint: "failCommand", mode: "off")
    GenServer.stop(admin)
  end

  # Session processes are linked to the topology, so a session that was never ended stays in this list.
  defp session_pids(top) do
    {:links, links} = Process.info(top, :links)

    Enum.filter(links, fn pid ->
      is_pid(pid) and match?({:initial_call, {Session, :init, 6}}, Process.info(pid, :initial_call))
    end)
  end

  # end_session replies before the session process exits, so give the exits a moment to land.
  defp assert_sessions_ended(top, before, attempts \\ 50) do
    leaked = session_pids(top) -- before

    cond do
      leaked == [] ->
        :ok

      attempts == 0 ->
        flunk("#{length(leaked)} session processes still alive")

      true ->
        receive do
        after
          20 -> assert_sessions_ended(top, before, attempts - 1)
        end
    end
  end
end
