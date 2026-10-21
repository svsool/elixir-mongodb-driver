defmodule Mongo.TransactionStepDownTest do
  # Needs a replica set with at least two electable members, for example
  # MONGO_STEP_DOWN_SEEDS=127.0.0.1:27217,127.0.0.1:27218,127.0.0.1:27219 MONGO_STEP_DOWN_SET=stepdown
  use ExUnit.Case, async: false

  @seeds System.get_env("MONGO_STEP_DOWN_SEEDS")
  @set_name System.get_env("MONGO_STEP_DOWN_SET")
  @coll "transaction_step_down"

  if is_nil(@seeds) or is_nil(@set_name), do: @moduletag(skip: "MONGO_STEP_DOWN_SEEDS and MONGO_STEP_DOWN_SET are not set")

  @tag timeout: 60_000
  test "a transaction rerun after a step-down waits for the new primary" do
    seeds = String.split(@seeds, ",")
    top = start_supervised!({Mongo, database: "mongodb_test", seeds: seeds, set_name: @set_name, appname: "transaction-step-down-test", name: :transaction_step_down_test})
    admin = start_supervised!(Supervisor.child_spec({Mongo, database: "admin", seeds: seeds, set_name: @set_name, name: :transaction_step_down_admin}, id: :admin))
    {:ok, _} = Mongo.update_one(top, @coll, %{_id: "fence"}, %{"$set": %{v: 0}}, upsert: true, w: :majority)

    test_pid = self()
    runs = :counters.new(1, [])

    # The session is bound to the primary before the step-down, and the first statement runs after it.
    callback = fn ->
      :counters.add(runs, 1, 1)

      if :counters.get(runs, 1) == 1 do
        send(test_pid, :session_bound)
        assert_receive :stepped_down, 5_000
      end

      case Mongo.update_one(top, @coll, %{_id: "fence"}, %{"$inc": %{v: 1}}) do
        {:ok, _} -> {:ok, :updated}
        {:error, error} -> {:error, error}
      end
    end

    transaction = Task.async(fn -> :timer.tc(fn -> Mongo.transaction(top, callback, w: :majority, checkout_timeout: 1_000, transaction_retry_timeout_s: 1, transaction_retry_jitter_ms: 10) end) end)
    assert_receive :session_bound, 5_000
    assert {:ok, _} = Mongo.admin_command(admin, replSetStepDown: 10, secondaryCatchUpPeriodSecs: 5)
    send(transaction.pid, :stepped_down)
    {elapsed_us, result} = Task.await(transaction, 10_000)

    IO.puts("step-down: #{inspect(summary(result))}, callback runs #{:counters.get(runs, 1)}, #{div(elapsed_us, 1000)} ms")
    assert {:ok, :updated} = result
    assert :counters.get(runs, 1) <= 5
  end

  defp summary({:error, %Mongo.Error{code: code, error_labels: labels}}), do: %{code: code, labels: labels}
  defp summary(result), do: result
end
