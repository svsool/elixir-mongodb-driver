defmodule Mongo.TransactionRerunTest do
  use CollectionCase

  alias Mongo.Events.CommandStartedEvent
  alias Mongo.Session

  @hold_ms 300

  @tag :rs_required
  test "transaction_retry_jitter_ms slows down the rerun loop after a write conflict", %{pid: top, catcher: catcher} do
    {without_jitter, _aborts} = conflict_run(top, catcher, "transaction_rerun_without_jitter", transaction_retry_jitter_ms: 0)
    {with_jitter, _aborts} = conflict_run(top, catcher, "transaction_rerun_with_jitter", transaction_retry_jitter_ms: 10)

    assert with_jitter > 0
    assert with_jitter * 2 < without_jitter
  end

  @tag :rs_required
  test "a rerun after a write conflict sends no abort", %{pid: top, catcher: catcher} do
    assert {reruns, 0} = conflict_run(top, catcher, "transaction_rerun_write_conflict", [])
    assert reruns > 1
  end

  @tag :rs_required
  test "a transient error that keeps the server transaction open still sends an abort", %{catcher: catcher} do
    app_name = "transaction-rerun-test"
    top = start_supervised!({Mongo, database: "mongodb_test", seeds: ["127.0.0.1:27017"], appname: app_name, name: :transaction_rerun_test})
    coll = "transaction_rerun_transient_error"
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: "fence", v: 0})
    data = [failCommands: ["update"], errorCode: 2, errorLabels: ["TransientTransactionError"], appName: app_name]
    assert {:ok, _} = Mongo.admin_command(top, configureFailPoint: "failCommand", mode: [times: 1], data: data)
    reset_events(catcher)

    assert {:ok, :updated} = Mongo.transaction(top, fn -> update_fence(top, coll) end, w: :majority)
    assert %{update: 2, abortTransaction: 1, commitTransaction: 1} = command_counts(catcher)
  end

  @tag :rs_required
  test "a rerun after a write conflict returned before any statement starts a new transaction", %{pid: top} do
    conflict = Mongo.Error.exception(%{"code" => 112, "errmsg" => "write conflict", "errorLabels" => ["TransientTransactionError"]})
    counter = :counters.new(1, [])

    callback = fn ->
      :counters.add(counter, 1, 1)
      if :counters.get(counter, 1) == 1, do: {:error, conflict}, else: {:ok, :done}
    end

    assert {:ok, :done} = Mongo.transaction(top, callback, w: :majority)
    assert :counters.get(counter, 1) == 2
  end

  @tag :rs_required
  test "mark_aborted works in every session state", %{pid: top} do
    coll = "transaction_rerun_mark_aborted"
    {:ok, session} = Session.start_session(top, :write, [])

    assert :ok = Session.mark_aborted(session)
    :ok = Session.start_transaction(session)
    assert :ok = Session.mark_aborted(session)
    :ok = Session.start_transaction(session)
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: 1}, session: session)
    assert :ok = Session.mark_aborted(session)
    assert :ok = Session.mark_aborted(session)
    :ok = Session.start_transaction(session)
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: 2}, session: session)
    assert :ok = Session.commit_transaction(session)
    assert :ok = Session.mark_aborted(session)
    assert :ok = Session.end_session(top, session)

    assert [%{"_id" => 2}] = top |> Mongo.find(coll, %{}) |> Enum.to_list()
  end

  # Another session holds an open transaction on the document, so each statement gets a real WriteConflict (112).
  defp conflict_run(top, catcher, coll, opts) do
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: "fence", v: 0})
    blocker = hold_document(top, coll)
    assert_receive :blocker_ready
    reset_events(catcher)

    assert {:ok, :updated} = Mongo.transaction(top, fn -> update_fence(top, coll) end, [w: :majority, transaction_retry_timeout_s: 5] ++ opts)
    assert {:ok, _} = Task.await(blocker)

    counts = command_counts(catcher)
    {Map.get(counts, :update, 0), Map.get(counts, :abortTransaction, 0)}
  end

  defp hold_document(top, coll) do
    parent = self()

    Task.async(fn ->
      {:ok, session} = Session.start_session(top, :write, [])
      :ok = Session.start_transaction(session)
      {:ok, _} = Mongo.update_one(top, coll, %{_id: "fence"}, %{"$inc" => %{v: 1}}, session: session)
      send(parent, :blocker_ready)
      Process.sleep(@hold_ms)
      result = Session.commit_transaction(session)
      Session.end_session(top, session)
      with :ok <- result, do: {:ok, :committed}
    end)
  end

  defp update_fence(top, coll) do
    with {:ok, _} <- Mongo.command(top, update: coll, updates: [%{q: %{_id: "fence"}, u: %{"$inc" => %{v: 1}}}]) do
      {:ok, :updated}
    end
  end

  defp reset_events(catcher) do
    :sys.replace_state(catcher, fn _events -> [] end)
  end

  defp command_counts(catcher) do
    catcher
    |> EventCatcher.events()
    |> Enum.flat_map(fn
      %CommandStartedEvent{command_name: name} -> [name]
      _other -> []
    end)
    |> Enum.frequencies()
  end
end
