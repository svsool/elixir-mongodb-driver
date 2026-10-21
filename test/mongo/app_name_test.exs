defmodule Mongo.AppNameTest do
  use ExUnit.Case, async: false

  @seeds ["127.0.0.1:27017"]

  setup do
    app_name = "app-name-test-#{System.unique_integer([:positive])}"
    other_app_name = "#{app_name}-other"
    own = start_supervised!({Mongo, database: "mongodb_test", seeds: @seeds, pool_size: 2, appname: app_name, name: :app_name_own}, id: :own)
    other = start_supervised!({Mongo, database: "mongodb_test", seeds: @seeds, pool_size: 2, appname: other_app_name, name: :app_name_other}, id: :other)
    {:ok, _} = Mongo.ping(own)
    {:ok, _} = Mongo.ping(other)
    %{own: own, other: other, app_name: app_name}
  end

  @tag :rs_required
  test "the server records the appname of every pool connection", %{own: own, app_name: app_name} do
    {:ok, %{"inprog" => ops}} = Mongo.admin_command(own, currentOp: 1, "$all": true)

    assert Enum.count(ops, fn op -> op["appName"] == app_name end) >= 2
  end

  @tag :rs_required
  test "a failpoint scoped to the appname fails only that client", %{own: own, other: other, app_name: app_name} do
    assert {:ok, _} = Mongo.admin_command(own, configureFailPoint: "failCommand", mode: [times: 1], data: [failCommands: ["find"], errorCode: 91, appName: app_name])

    assert nil == Mongo.find_one(other, "app_name_test", %{})
    assert {:error, %Mongo.Error{code: 91}} = Mongo.find_one(own, "app_name_test", %{}, retryable_reads: false)
    assert {:ok, _} = Mongo.admin_command(other, configureFailPoint: "failCommand", mode: "off")
  end
end
