defmodule Mongo.CheckoutTimeoutTest do
  use ExUnit.Case, async: false

  alias Mongo.Error

  @checkout_timeout 500
  @selection_error "server selection timeout after #{@checkout_timeout} ms"

  describe "without a reachable server" do
    setup do
      top = start_supervised!({Mongo, database: "mongodb_test", seeds: ["127.0.0.1:1"], pool_size: 1, name: :checkout_timeout_unreachable})
      %{top: top}
    end

    test "transaction returns a server selection error", %{top: top} do
      assert_selection_error(fn -> Mongo.transaction(top, fn -> {:ok, :done} end, checkout_timeout: @checkout_timeout) end)
    end

    test "command returns a server selection error", %{top: top} do
      assert_selection_error(fn -> Mongo.command(top, [ping: 1], checkout_timeout: @checkout_timeout) end)
    end

    test "find_one returns a server selection error", %{top: top} do
      assert_selection_error(fn -> Mongo.find_one(top, "checkout_timeout_test", %{}, checkout_timeout: @checkout_timeout) end)
    end

    test "insert_one returns a server selection error", %{top: top} do
      assert_selection_error(fn -> Mongo.insert_one(top, "checkout_timeout_test", %{name: "Greta"}, checkout_timeout: @checkout_timeout) end)
    end

    test "find returns a server selection error", %{top: top} do
      assert_selection_error(fn -> Mongo.find(top, "checkout_timeout_test", %{}, checkout_timeout: @checkout_timeout) end)
    end

    test "watch_collection returns a server selection error", %{top: top} do
      assert_selection_error(fn -> Mongo.watch_collection(top, "checkout_timeout_test", [], fn _token -> :ok end, checkout_timeout: @checkout_timeout) end)
    end
  end

  test "checkout_timeout does not change a call on a reachable server" do
    {:ok, top} = Mongo.TestConnection.connect()

    assert {:ok, :done} = Mongo.transaction(top, fn -> {:ok, :done} end, checkout_timeout: @checkout_timeout)
    assert {:ok, _} = Mongo.command(top, [ping: 1], checkout_timeout: @checkout_timeout)
  end

  defp assert_selection_error(fun) do
    {elapsed_us, result} = :timer.tc(fun)

    assert {:error, %Error{message: @selection_error}} = result
    assert div(elapsed_us, 1000) in @checkout_timeout..(@checkout_timeout + 1_000)
  end
end
