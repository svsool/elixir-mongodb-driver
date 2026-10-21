defmodule Mongo.CommitWriteConcernTest do
  use CollectionCase

  alias Mongo.Error
  alias Mongo.Events.CommandStartedEvent
  alias Mongo.Session

  @app_name "commit-write-concern-test"
  @write_concern_error [writeConcernError: [code: 64, codeName: "WriteConcernFailed", errmsg: "waiting for replication timed out", errInfo: [wtimeout: true]]]

  setup do
    top = start_supervised!({Mongo, database: "mongodb_test", seeds: ["127.0.0.1:27017"], appname: @app_name, name: :commit_write_concern_test})
    {:ok, _} = Mongo.ping(top)
    %{top: top}
  end

  @tag :rs_required
  test "a commit with writeConcernError returns an error with the unknown commit label and sends no abort", %{top: top, catcher: catcher} do
    coll = unique_collection()
    fail_commit(top, @write_concern_error)

    assert {:error, %Error{code: 64} = error} = insert_in_transaction(top, coll, %{_id: 1}, w: :majority)
    assert Error.has_label(error, "UnknownTransactionCommitResult")
    assert %{commitTransaction: 1, abortTransaction: 0} = command_counts(catcher)
    assert %{"_id" => 1} = Mongo.find_one(top, coll, %{_id: 1})
  end

  @tag :rs_required
  test "a commit retry after an unknown commit result still succeeds", %{top: top, catcher: catcher} do
    coll = unique_collection()
    fail_commit(top, errorCode: 91, errorLabels: ["UnknownTransactionCommitResult"])

    assert {:ok, :inserted} = insert_in_transaction(top, coll, %{_id: 1}, w: :majority)
    assert %{commitTransaction: 2, abortTransaction: 0} = command_counts(catcher)
    assert [%{w: :majority}, %{w: :majority, wtimeout: 10_000}] == commit_write_concerns(catcher)
  end

  @tag :rs_required
  test "a commit with an unsatisfiable write concern returns an error with the unknown commit label and sends no abort", %{top: top, catcher: catcher} do
    coll = unique_collection()

    assert {:error, %Error{code: 100} = error} = insert_in_transaction(top, coll, %{_id: 1}, w: 50)
    assert Error.has_label(error, "UnknownTransactionCommitResult")
    assert %{commitTransaction: 1, abortTransaction: 0} = command_counts(catcher)
  end

  @tag :rs_required
  test "a repeated commit after an unsatisfiable write concern succeeds with a majority write concern and sends no abort", %{top: top, catcher: catcher} do
    coll = unique_collection()
    session = session_with_insert(top, coll, w: 50)

    assert {:error, %Error{code: 100}} = Session.commit_transaction(session)
    assert :ok = Session.commit_transaction(session)
    assert :ok = Session.abort_transaction(session)
    assert %{commitTransaction: 2, abortTransaction: 0} = command_counts(catcher)
    assert %{"_id" => 1} = Mongo.find_one(top, coll, %{_id: 1})
    Session.end_session(top, session)
  end

  @tag :rs_required
  test "a repeated commit after a writeConcernError confirms the commit with a majority write concern and the default wtimeout", %{top: top, catcher: catcher} do
    coll = unique_collection()
    session = session_with_insert(top, coll, w: 1)
    fail_commit(top, @write_concern_error)

    assert {:error, %Error{code: 64}} = Session.commit_transaction(session)
    assert :ok = Session.commit_transaction(session)
    assert [%{w: 1}, %{w: :majority, wtimeout: 10_000}] == commit_write_concerns(catcher)
    assert %{commitTransaction: 2, abortTransaction: 0} = command_counts(catcher)
    assert %{"_id" => 1} = Mongo.find_one(top, coll, %{_id: 1})
    Session.end_session(top, session)
  end

  @tag :rs_required
  test "a repeated commit keeps the configured wtimeout and journal settings", %{top: top, catcher: catcher} do
    coll = unique_collection()
    session = session_with_insert(top, coll, w: 1, j: true, wtimeout: 2_000)
    fail_commit(top, @write_concern_error)

    assert {:error, %Error{code: 64}} = Session.commit_transaction(session)
    assert :ok = Session.commit_transaction(session)
    assert [%{w: 1, j: true, wtimeout: 2_000}, %{w: :majority, j: true, wtimeout: 2_000}] == commit_write_concerns(catcher)
    Session.end_session(top, session)
  end

  @tag :rs_required
  test "a failed commit still sends an abort", %{top: top, catcher: catcher} do
    coll = unique_collection()
    fail_commit(top, errorCode: 50)

    assert {:error, %Error{code: 50}} = insert_in_transaction(top, coll, %{_id: 1}, w: :majority)
    assert %{commitTransaction: 1, abortTransaction: 1} = command_counts(catcher)
  end

  defp fail_commit(top, data) do
    assert {:ok, _} = Mongo.admin_command(top, configureFailPoint: "failCommand", mode: [times: 1], data: [failCommands: ["commitTransaction"], appName: @app_name] ++ data)
  end

  defp session_with_insert(top, coll, opts) do
    {:ok, session} = Session.start_session(top, :write, opts)
    :ok = Session.start_transaction(session)
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: 1}, session: session)
    session
  end

  defp insert_in_transaction(top, coll, doc, opts) do
    Mongo.transaction(
      top,
      fn ->
        with {:ok, _} <- Mongo.insert_one(top, coll, doc) do
          {:ok, :inserted}
        end
      end,
      opts
    )
  end

  defp command_counts(catcher) do
    counts =
      catcher
      |> EventCatcher.events()
      |> Enum.flat_map(fn
        %CommandStartedEvent{command_name: name} -> [name]
        _other -> []
      end)
      |> Enum.frequencies()

    Map.merge(%{commitTransaction: 0, abortTransaction: 0}, counts)
  end

  defp commit_write_concerns(catcher) do
    catcher
    |> EventCatcher.events()
    |> Enum.reverse()
    |> Enum.flat_map(fn
      %CommandStartedEvent{command_name: :commitTransaction, command: command} -> [command[:writeConcern]]
      _other -> []
    end)
  end
end
