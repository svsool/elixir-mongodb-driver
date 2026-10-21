defmodule Mongo.SessionStateTest do
  use CollectionCase

  alias Mongo.Error
  alias Mongo.Events.CommandStartedEvent
  alias Mongo.Session

  setup do
    top = start_supervised!({Mongo, database: "mongodb_test", seeds: ["127.0.0.1:27017"], name: :session_state_test})
    {:ok, _} = Mongo.ping(top)
    {:ok, session} = Session.start_session(top, :write, w: :majority)
    on_exit(fn -> if Process.alive?(session), do: Session.end_session(top, session) end)
    %{top: top, session: session}
  end

  @tag :rs_required
  test "a commit without a transaction returns an error and sends no command", %{session: session, catcher: catcher} do
    assert {:error, %Error{message: "no transaction started"}} = Session.commit_transaction(session)
    assert 0 = commit_count(catcher)
  end

  @tag :rs_required
  test "a commit after an abort returns an error and stores nothing", %{top: top, session: session, catcher: catcher} do
    coll = unique_collection()
    :ok = Mongo.create(top, coll)
    :ok = Session.start_transaction(session)
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: 1}, session: session)
    :ok = Session.abort_transaction(session)

    assert {:error, %Error{message: "cannot commit an aborted transaction"}} = Session.commit_transaction(session)
    assert 0 = commit_count(catcher)
    assert nil == Mongo.find_one(top, coll, %{_id: 1})
  end

  @tag :rs_required
  test "start_transaction during a transaction returns an error and keeps the transaction", %{top: top, session: session} do
    coll = unique_collection()
    :ok = Mongo.create(top, coll)
    :ok = Session.start_transaction(session)
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: 1}, session: session)

    assert {:error, %Error{message: "transaction already in progress"}} = Session.start_transaction(session)
    assert Process.alive?(top)
    assert :ok = Session.commit_transaction(session)
    assert %{"_id" => 1} = Mongo.find_one(top, coll, %{_id: 1})
  end

  @tag :rs_required
  test "an unsupported session call returns an error and keeps the topology alive", %{top: top, session: session} do
    send(session, {:call, self(), :unsupported})

    assert_receive {:session_result, {:error, %Error{message: "unsupported session call :unsupported"}}}
    assert Process.alive?(session)
    assert Process.alive?(top)
  end

  defp commit_count(catcher) do
    catcher
    |> EventCatcher.events()
    |> Enum.count(&match?(%CommandStartedEvent{command_name: :commitTransaction}, &1))
  end
end
