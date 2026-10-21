defmodule Mongo.SessionExitTest do
  use ExUnit.Case, async: true

  alias Mongo.Error
  alias Mongo.Session

  test "a call returns an error when the session process exits before it replies" do
    session = spawn(fn -> receive do: ({:call, _from, _cmd} -> exit(:boom)) end)

    task = Task.async(fn -> Session.commit_transaction(session) end)

    assert {:ok, {:error, %Error{message: "session process exited"}}} = Task.yield(task, 1_000) || Task.shutdown(task, :brutal_kill)
  end

  test "a call to a session process that is already gone returns an error" do
    session = spawn(fn -> :ok end)
    ref = Process.monitor(session)
    assert_receive {:DOWN, ^ref, :process, _pid, _reason}

    assert {:error, %Error{message: "session process exited"}} = Session.abort_transaction(session)
  end
end
