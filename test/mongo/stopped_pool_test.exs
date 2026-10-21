defmodule Mongo.StoppedPoolTest do
  use ExUnit.Case, async: false

  alias Mongo.Error
  alias Mongo.Session

  # A separate database: the server transactions these tests cannot end must not block dropDatabase of the shared one.
  @database "mongodb_test_stopped_pool"

  setup do
    top = start_supervised!({Mongo, database: @database, seeds: ["127.0.0.1:27017"], name: :stopped_pool_test})
    assert {:ok, _} = Mongo.command(top, ping: 1)
    %{top: top}
  end

  @tag :rs_required
  test "a commit on a pool that the topology closed returns an error and keeps the session and the topology alive", %{top: top} do
    session = session_with_open_transaction(top, "stopped_pool_commit")
    topology_ref = Process.monitor(top)
    close_session_pool(top, session)

    assert {:error, %Error{message: "connection pool not available: noproc"}} = Session.commit_transaction(session)
    assert Process.alive?(session)
    refute_receive {:DOWN, ^topology_ref, :process, _pid, _reason}, 200
  end

  @tag :rs_required
  test "an abort on a pool that the topology closed keeps the session and the topology alive", %{top: top} do
    session = session_with_open_transaction(top, "stopped_pool_abort")
    topology_ref = Process.monitor(top)
    close_session_pool(top, session)

    assert :ok = Session.abort_transaction(session)
    assert Process.alive?(session)
    refute_receive {:DOWN, ^topology_ref, :process, _pid, _reason}, 200
  end

  @tag :rs_required
  test "a commit retry on a pool that the topology closed keeps the unknown outcome label", %{top: top} do
    session = session_with_open_transaction(top, "stopped_pool_commit_retry")
    on_exit(&fail_point_off/0)
    {:ok, _} = Mongo.admin_command(top, configureFailPoint: "failCommand", mode: [times: 1], data: [failCommands: ["commitTransaction"], errorCode: 6, errorLabels: ["UnknownTransactionCommitResult"]])
    config = %{top: top, session: session, pool: Session.connection(session)}
    :ok = :telemetry.attach(__MODULE__, [:mongodb_driver, :execution], &__MODULE__.close_pool_after_commit/4, config)
    on_exit(fn -> :telemetry.detach(__MODULE__) end)

    assert {:error, %Error{message: "connection pool not available: noproc"} = error} = Session.commit_transaction(session)
    assert Error.has_label(error, "UnknownTransactionCommitResult")
  end

  @tag :rs_required
  test "a statement on a pool that the topology closed makes Mongo.transaction return an error", %{top: top} do
    topology_ref = Process.monitor(top)

    callback = fn ->
      close_session_pool(top, Process.get(:session))
      Mongo.insert_one(top, "stopped_pool_statement", %{_id: 1})
    end

    assert {:error, %Error{message: "connection pool not available: noproc"}} = Mongo.transaction(top, callback, w: :majority)
    refute_receive {:DOWN, ^topology_ref, :process, _pid, _reason}, 200
  end

  defp session_with_open_transaction(top, coll) do
    {:ok, session} = Session.start_session(top, :write, w: :majority)
    :ok = Session.start_transaction(session)
    {:ok, _} = Mongo.insert_one(top, coll, %{v: 1}, session: session)
    session
  end

  # The driver logs a command in the calling process after the checkin, so the pool closes between the first commit and its retry.
  def close_pool_after_commit(_event, _measurements, %{command: :commitTransaction}, %{top: top, session: session, pool: pool}) do
    if self() == session, do: close_pool(top, pool)
  end

  def close_pool_after_commit(_event, _measurements, _metadata, _config), do: :ok

  defp close_session_pool(top, session), do: close_pool(top, Session.connection(session))

  # The topology closes a server's pool when the monitor of that server disconnects, while sessions still hold the pool.
  defp close_pool(top, pool) do
    %{monitors: monitors, connection_pools: pools} = :sys.get_state(top)
    {address, ^pool} = Enum.find(pools, fn {_address, pid} -> pid == pool end)
    GenServer.cast(top, {:disconnect, :monitor, address, monitors[address]})
    _state = :sys.get_state(top)
    refute Process.alive?(pool)
  end

  defp fail_point_off do
    {:ok, admin} = Mongo.start_link(database: "admin", seeds: ["127.0.0.1:27017"])
    Mongo.admin_command(admin, configureFailPoint: "failCommand", mode: "off")
    GenServer.stop(admin)
  end
end
