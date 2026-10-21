defmodule Mongo.LateCheckoutTest do
  use ExUnit.Case, async: false
  use Patch

  alias Mongo.Error
  alias Mongo.Session.ServerSession
  alias Mongo.Topology

  @checkout_timeout 100
  @selection_error "server selection timeout after #{@checkout_timeout} ms"

  setup do
    top = start_supervised!({Mongo, database: "mongodb_test", seeds: ["127.0.0.1:27017"], name: :late_checkout_test})
    assert {:ok, _} = Mongo.command(top, ping: 1)
    %{top: top}
  end

  test "a checkout_session request handled after its caller timed out starts no session", %{top: top} do
    :ok = :sys.suspend(top)
    assert {:error, %Error{message: @selection_error}} = Topology.checkout_session(top, :write, checkout_timeout: @checkout_timeout)
    :ok = :sys.resume(top)
    _state = :sys.get_state(top)

    assert linked_sessions(top) == []
  end

  test "a session started after its caller timed out is ended and its server session returns to the pool", %{top: top} do
    pooled = length(:sys.get_state(top).session_pool.queue)

    # The server session pool asks this after the deadline check, so the session starts after the caller gave up.
    patch(ServerSession, :about_to_expire?, fn _session, _timeout -> not pause(2 * @checkout_timeout) end)

    assert {:error, %Error{message: @selection_error}} = Topology.checkout_session(top, :write, checkout_timeout: @checkout_timeout)
    # Returns only after the topology handled the delayed checkout.
    _state = :sys.get_state(top)

    assert_eventually(fn -> linked_sessions(top) == [] end)
    assert_eventually(fn -> length(:sys.get_state(top).session_pool.queue) == pooled end)
  end

  test "a delivered session leaves no checkout entry once it ends", %{top: top} do
    assert {:ok, session} = Topology.checkout_session(top, :write, [])
    assert :ok = Mongo.Session.end_session(top, session)

    assert_eventually(fn -> :sys.get_state(top).session_checkouts == %{} end)
  end

  defp linked_sessions(top) do
    {:links, links} = Process.info(top, :links)
    Enum.filter(links, fn pid -> is_pid(pid) and Process.info(pid, :initial_call) == {:initial_call, {Mongo.Session, :init, 6}} end)
  end

  defp assert_eventually(check, attempts \\ 50) do
    cond do
      check.() ->
        :ok

      attempts == 0 ->
        flunk("condition not reached")

      true ->
        pause(20)
        assert_eventually(check, attempts - 1)
    end
  end

  defp pause(ms) do
    receive do
    after
      ms -> true
    end
  end
end
