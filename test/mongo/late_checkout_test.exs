defmodule Mongo.LateCheckoutTest do
  use ExUnit.Case, async: false

  alias Mongo.Error
  alias Mongo.Topology

  @checkout_timeout 100
  @selection_error "server selection timeout after #{@checkout_timeout} ms"

  test "a checkout_session request handled after its caller timed out starts no session" do
    top = start_supervised!({Mongo, database: "mongodb_test", seeds: ["127.0.0.1:27017"], name: :late_checkout_test})
    assert {:ok, _} = Mongo.command(top, ping: 1)

    :ok = :sys.suspend(top)
    assert {:error, %Error{message: @selection_error}} = Topology.checkout_session(top, :write, checkout_timeout: @checkout_timeout)
    :ok = :sys.resume(top)
    _state = :sys.get_state(top)

    assert linked_sessions(top) == []
  end

  defp linked_sessions(top) do
    {:links, links} = Process.info(top, :links)
    Enum.filter(links, fn pid -> is_pid(pid) and Process.info(pid, :initial_call) == {:initial_call, {Mongo.Session, :init, 6}} end)
  end
end
