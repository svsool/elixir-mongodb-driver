defmodule Mongo.WriteConcernErrorTest do
  use CollectionCase

  alias Mongo.Error

  @app_name "write-concern-error-test"
  @write_concern_error [writeConcernError: [code: 64, codeName: "WriteConcernFailed", errmsg: "waiting for replication timed out", errInfo: [wtimeout: true]]]

  setup %{pid: pid} do
    top = start_supervised!({Mongo, database: "mongodb_test", seeds: ["127.0.0.1:27017"], appname: @app_name, name: :write_concern_error_test})
    {:ok, _} = Mongo.ping(top)
    on_exit(fn -> {:ok, _} = Mongo.admin_command(pid, configureFailPoint: "failCommand", mode: "off") end)
    %{top: top}
  end

  @tag :rs_required
  test "insert_one with a writeConcernError returns the marked error", %{top: top} do
    coll = unique_collection()
    fail(top, "insert", @write_concern_error)

    assert {:error, %Error{code: 64, write_concern_error: true, error_info: %{"wtimeout" => true}}} = Mongo.insert_one(top, coll, %{_id: 1})
  end

  @tag :rs_required
  test "update_one with a writeConcernError returns the marked error", %{top: top} do
    coll = unique_collection()
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: 1})
    fail(top, "update", @write_concern_error)

    assert {:error, %Error{code: 64, write_concern_error: true}} = Mongo.update_one(top, coll, %{_id: 1}, %{"$set" => %{name: "Greta"}})
  end

  @tag :rs_required
  test "delete_many with a writeConcernError returns the marked error", %{top: top} do
    coll = unique_collection()
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: 1})
    fail(top, "delete", @write_concern_error)

    assert {:error, %Error{code: 64, write_concern_error: true}} = Mongo.delete_many(top, coll, %{})
  end

  @tag :rs_required
  test "find_one_and_update with a writeConcernError returns the marked error", %{top: top} do
    coll = unique_collection()
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: 1})
    fail(top, "findAndModify", @write_concern_error)

    assert {:error, %Error{code: 64, write_concern_error: true}} = Mongo.find_one_and_update(top, coll, %{_id: 1}, %{"$set" => %{name: "Greta"}})
  end

  @tag :rs_required
  test "a raw command with a writeConcernError returns the marked error", %{top: top} do
    coll = unique_collection()
    fail(top, "insert", @write_concern_error)

    assert {:error, %Error{code: 64, write_concern_error: true}} = Mongo.command(top, insert: coll, documents: [%{_id: 1}])
  end

  @tag :rs_required
  test "a writeConcernError wins over writeErrors in the same reply", %{top: top, catcher: catcher} do
    coll = unique_collection()
    {:ok, _} = Mongo.insert_one(top, coll, %{_id: 1})
    fail(top, "insert", @write_concern_error)

    assert {:error, %Error{code: 64, write_concern_error: true}} = Mongo.insert_one(top, coll, %{_id: 1})

    assert catcher
           |> EventCatcher.succeeded_events()
           |> Enum.any?(&match?(%{command_name: :insert, reply: %{"writeErrors" => [%{"code" => 11_000} | _], "writeConcernError" => %{"code" => 64}}}, &1))
  end

  @tag :rs_required
  test "the same writes without a writeConcernError keep their results", %{top: top} do
    coll = unique_collection()

    assert {:ok, %Mongo.InsertOneResult{inserted_id: 1}} = Mongo.insert_one(top, coll, %{_id: 1})
    assert {:error, %Mongo.WriteError{}} = Mongo.insert_one(top, coll, %{_id: 1})
    assert {:ok, %Mongo.UpdateResult{modified_count: 1}} = Mongo.update_one(top, coll, %{_id: 1}, %{"$set" => %{name: "Greta"}})
    assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} = Mongo.delete_many(top, coll, %{})
  end

  defp fail(top, command, data) do
    assert {:ok, _} = Mongo.admin_command(top, configureFailPoint: "failCommand", mode: [times: 1], data: [failCommands: [command], appName: @app_name] ++ data)
  end
end
