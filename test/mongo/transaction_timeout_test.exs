defmodule Mongo.TransactionTimeoutTest do
  use CollectionCase

  @app_name "transaction-timeout-test"
  @block_ms 3_000
  @timeout 1_000

  setup do
    top = start_supervised!({Mongo, database: "mongodb_test", seeds: ["127.0.0.1:27017"], appname: @app_name, name: :transaction_timeout_test})
    {:ok, _} = Mongo.ping(top)
    %{top: top}
  end

  @tag :rs_required
  test "a blocked commit ends after the transaction timeout", %{top: top} do
    coll = unique_collection()
    block(top, "commitTransaction", @block_ms)

    {elapsed_ms, result} = timed(fn -> Mongo.transaction(top, insert(top, coll, 1), timeout: @timeout, w: :majority) end)

    assert {:error, %Mongo.Error{code: nil, host: host} = error} = result
    assert is_binary(host)
    assert Mongo.Error.has_label(error, "UnknownTransactionCommitResult")
    assert elapsed_ms in @timeout..(@block_ms - 500)
    assert_not_stored_after_block(top, coll, 1)
  end

  @tag :rs_required
  test "a blocked abort ends after the transaction timeout", %{top: top} do
    coll = unique_collection()
    block(top, "abortTransaction", @block_ms)

    callback = fn ->
      {:ok, _} = Mongo.insert_one(top, coll, %{_id: 1})
      {:error, :callback_failed}
    end

    {elapsed_ms, result} = timed(fn -> Mongo.transaction(top, callback, timeout: @timeout, w: :majority) end)

    assert {:error, :callback_failed} = result
    assert elapsed_ms in @timeout..(@block_ms - 500)
    assert_not_stored_after_block(top, coll, 1)
  end

  @tag :rs_required
  test "a commit with a timeout and no block succeeds", %{top: top} do
    coll = unique_collection()

    assert {:ok, :inserted} = Mongo.transaction(top, insert(top, coll, 1), timeout: @timeout, w: :majority)
  end

  @tag :rs_required
  test "a blocked commit without a timeout waits for the server", %{top: top} do
    coll = unique_collection()
    block(top, "commitTransaction", 2_000)

    {elapsed_ms, result} = timed(fn -> Mongo.transaction(top, insert(top, coll, 1), w: :majority) end)

    assert {:ok, :inserted} = result
    assert elapsed_ms >= 2_000
  end

  defp block(top, command, block_ms) do
    data = [failCommands: [command], blockConnection: true, blockTimeMS: block_ms, appName: @app_name]
    assert {:ok, _} = Mongo.admin_command(top, configureFailPoint: "failCommand", mode: [times: 1], data: data)
  end

  defp insert(top, coll, id) do
    fn ->
      with {:ok, _} <- Mongo.insert_one(top, coll, %{_id: id}) do
        {:ok, :inserted}
      end
    end
  end

  defp timed(fun) do
    {elapsed_us, result} = :timer.tc(fun)
    {div(elapsed_us, 1000), result}
  end

  defp assert_not_stored_after_block(top, coll, id) do
    Process.sleep(@block_ms)
    assert {:ok, 0} = Mongo.count_documents(top, coll, %{_id: id})
  end
end
