defmodule Mongo.RetryableWritesTest do
  use CollectionCase

  alias Mongo.Error
  alias Mongo.Events.CommandStartedEvent

  @app_name "retryable-writes-test"

  setup %{pid: pid} do
    top = start_supervised!({Mongo, database: "mongodb_test", seeds: ["127.0.0.1:27017"], appname: @app_name, name: :retryable_writes_test})
    {:ok, _} = Mongo.ping(top)
    on_exit(fn -> {:ok, _} = Mongo.admin_command(pid, configureFailPoint: "failCommand", mode: "off") end)
    %{top: top}
  end

  @tag :rs_required
  test "retryable writes: insert one is retried only with retryable_writes: true", %{top: top, catcher: catcher} do
    coll = unique_collection()

    fail(top, "insert", errorCode: 6)
    assert {:error, %Error{code: 6, retryable_writes: true}} = Mongo.insert_one(top, coll, %{"name" => "Waldo"}, retryable_writes: false)

    fail(top, "insert", errorCode: 6)
    assert {:error, %Error{code: 6, retryable_writes: true}} = Mongo.insert_one(top, coll, %{"name" => "Waldo"})

    fail(top, "insert", errorCode: 6)
    assert {:ok, _} = Mongo.insert_one(top, coll, %{"name" => "Waldo"}, retryable_writes: true)

    assert 4 == length(commands(catcher, :insert))
    assert [:insert] = EventCatcher.retry_write_events(catcher) |> Enum.map(fn event -> event.command_name end)
  end

  @tag :rs_required
  test "retryable writes: delete one is retried only with retryable_writes: true", %{top: top, catcher: catcher} do
    coll = unique_collection()
    {:ok, _} = Mongo.insert_one(top, coll, %{"name" => "Waldo"})

    fail(top, "delete", errorCode: 6)
    assert {:error, %Error{code: 6, retryable_writes: true}} = Mongo.delete_one(top, coll, %{"name" => "Waldo"}, retryable_writes: false)

    fail(top, "delete", errorCode: 6)
    assert {:error, %Error{code: 6, retryable_writes: true}} = Mongo.delete_one(top, coll, %{"name" => "Waldo"})

    fail(top, "delete", errorCode: 6)
    assert {:ok, _} = Mongo.delete_one(top, coll, %{"name" => "Waldo"}, retryable_writes: true)

    assert 4 == length(commands(catcher, :delete))
    assert [:delete] = EventCatcher.retry_write_events(catcher) |> Enum.map(fn event -> event.command_name end)
  end

  for code <- [189, 10_107, 262] do
    @tag :rs_required
    test "a write without the retryable_writes option and code #{code} is sent once and returns the error", %{top: top, catcher: catcher} do
      coll = unique_collection()
      fail(top, "insert", errorCode: unquote(code))

      assert {:error, %Error{code: unquote(code)}} = Mongo.insert_one(top, coll, %{_id: 1})
      assert [command] = commands(catcher, :insert)
      assert nil == command[:txnNumber]
    end
  end

  @tag :rs_required
  test "a write without the retryable_writes option and a writeConcernError 11602 is sent once and returns the marked error", %{top: top, catcher: catcher} do
    coll = unique_collection()
    fail(top, "insert", writeConcernError: [code: 11_602, codeName: "InterruptedDueToReplStateChange", errmsg: "operation was interrupted"])

    assert {:error, %Error{code: 11_602, write_concern_error: true, retryable_writes: true}} = Mongo.insert_one(top, coll, %{_id: 1})
    assert [_command] = commands(catcher, :insert)
  end

  @tag :rs_required
  test "a w: 0 write with retryable_writes: true gets no txnNumber, an acknowledged one does", %{top: top, catcher: catcher} do
    coll = unique_collection()

    assert {:ok, %Mongo.InsertOneResult{acknowledged: false}} = Mongo.insert_one(top, coll, %{_id: 1}, retryable_writes: true, w: 0)
    assert {:ok, %Mongo.InsertOneResult{acknowledged: true}} = Mongo.insert_one(top, coll, %{_id: 2}, retryable_writes: true, w: 1)

    assert [nil, %BSON.LongNumber{}] = catcher |> commands(:insert) |> Enum.map(& &1[:txnNumber])
  end

  @tag :rs_required
  test "a statement inside a transaction with code 10107 is sent once", %{top: top, catcher: catcher} do
    coll = unique_collection()
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: 0})
    fail(top, "insert", errorCode: 10_107, errorLabels: [])

    assert {:error, %Error{code: 10_107}} = Mongo.transaction(top, fn -> Mongo.insert_one(top, coll, %{_id: 1}) end, w: :majority)
    assert 1 == catcher |> commands(:insert) |> Enum.count(&(&1[:autocommit] == false))
  end

  test "retryable writes: replace one", %{pid: top} do
    coll = unique_collection()

    assert {:ok, %Mongo.InsertOneResult{acknowledged: true, inserted_id: 42}} = Mongo.insert_one(top, coll, %{"_id" => 42, "name" => "Waldo"}, retryable_writes: true)
    assert %{"_id" => 42, "name" => "Waldo"} = Mongo.find_one(top, coll, %{"_id" => 42})
    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 1}} = Mongo.replace_one(top, coll, %{"_id" => 42}, %{"_id" => 42, "name" => "Greta"}, retryable_writes: true)
    assert %{"_id" => 42, "name" => "Greta"} = Mongo.find_one(top, coll, %{"_id" => 42})
    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 1}} = Mongo.replace_one(top, coll, %{"_id" => 42}, %{"_id" => 42, "name" => "Merlin"}, retryable_writes: true)
    assert %{"_id" => 42, "name" => "Merlin"} = Mongo.find_one(top, coll, %{"_id" => 42})
  end

  defp fail(top, command, data) do
    assert {:ok, _} = Mongo.admin_command(top, configureFailPoint: "failCommand", mode: [times: 1], data: [failCommands: [command], appName: @app_name] ++ data)
  end

  defp commands(catcher, name) do
    catcher
    |> EventCatcher.events()
    |> Enum.reverse()
    |> Enum.flat_map(fn
      %CommandStartedEvent{command_name: ^name, command: command} -> [command]
      _other -> []
    end)
  end
end
