defmodule Mongo.CommitRetryLabelTest do
  use ExUnit.Case, async: true

  alias Mongo.Error
  alias Mongo.Session
  alias Mongo.Session.ServerSession

  # Answers each command with the next scripted reply and reports the command name to the test.
  defmodule ScriptedConnection do
    use DBConnection

    def connect(opts), do: {:ok, %{replies: opts[:replies], test_pid: opts[:test_pid]}}
    def checkout(state), do: {:ok, state}
    def ping(state), do: {:ok, state}
    def disconnect(_error, _state), do: :ok
    def handle_status(_opts, state), do: {:idle, state}
    def handle_prepare(_query, _opts, state), do: unused(state)
    def handle_close(_query, _opts, state), do: unused(state)
    def handle_begin(_opts, state), do: unused(state)
    def handle_commit(_opts, state), do: unused(state)
    def handle_rollback(_opts, state), do: unused(state)
    def handle_declare(_query, _params, _opts, state), do: unused(state)
    def handle_fetch(_query, _cursor, _opts, state), do: unused(state)
    def handle_deallocate(_query, _cursor, _opts, state), do: unused(state)

    def handle_execute(%Mongo.Query{action: {:command, [{command_name, _} | _]}} = query, _params, _opts, %{replies: [reply | replies]} = state) do
      send(state.test_pid, {:command, command_name})
      {:ok, query, {reply, %{command_name: command_name}, 0, 0}, %{state | replies: replies}}
    end

    defp unused(state), do: {:error, RuntimeError.exception("not used by the commit path"), state}
  end

  @unknown_result %{"ok" => 0, "code" => 91, "errmsg" => "shutting down", "errorLabels" => ["UnknownTransactionCommitResult"]}
  @no_such_transaction %{"ok" => 0, "code" => 251, "errmsg" => "no such transaction"}

  test "a coded error on the retried commit gets the unknown commit label" do
    session = session_in_transaction([@unknown_result, @no_such_transaction])

    assert {:error, %Error{code: 251} = error} = Session.commit_transaction(session)
    assert Error.has_label(error, "UnknownTransactionCommitResult")
    assert_received {:command, :commitTransaction}
    assert_received {:command, :commitTransaction}
  end

  test "the same coded error on the first commit gets no label" do
    session = session_in_transaction([@no_such_transaction])

    assert {:error, %Error{code: 251, error_labels: []}} = Session.commit_transaction(session)
  end

  test "a first commit error with the RetryableWriteError label gets the unknown commit label and is not retried" do
    session = session_in_transaction([%{"ok" => 0, "code" => 251, "errmsg" => "no such transaction", "errorLabels" => ["RetryableWriteError"]}])

    assert {:error, %Error{code: 251} = error} = Session.commit_transaction(session)
    assert Error.has_label(error, "UnknownTransactionCommitResult")
    assert_received {:command, :commitTransaction}
    refute_received {:command, :commitTransaction}
  end

  test "a commit error that already has the unknown commit label keeps one copy of it" do
    session = session_in_transaction([@unknown_result], transaction_retry_timeout_s: 0)

    assert {:error, %Error{code: 91, error_labels: ["UnknownTransactionCommitResult"]}} = Session.commit_transaction(session)
    assert_received {:command, :commitTransaction}
    refute_received {:command, :commitTransaction}
  end

  test "a commit writeConcernError that already has the unknown commit label keeps one copy of it" do
    session = session_in_transaction([%{"ok" => 1, "writeConcernError" => %{"code" => 64, "errmsg" => "waiting for replication timed out"}, "errorLabels" => ["UnknownTransactionCommitResult"]}])

    assert {:error, %Error{code: 64, write_concern_error: true, error_labels: ["UnknownTransactionCommitResult"]}} = Session.commit_transaction(session)
  end

  test "a retryable commit error with TransientTransactionError keeps only that label" do
    session = session_in_transaction([%{"ok" => 0, "code" => 6, "errmsg" => "host unreachable", "errorLabels" => ["TransientTransactionError"]}])

    assert {:error, %Error{code: 6, error_labels: ["TransientTransactionError"]}} = Session.commit_transaction(session)
  end

  defp session_in_transaction(replies, opts \\ []) do
    conn = start_supervised!(%{id: ScriptedConnection, start: {DBConnection, :start_link, [ScriptedConnection, [replies: replies, test_pid: self(), backoff_type: :stop]]}})
    {:ok, session} = Session.start_link(self(), conn, "scripted:27017", ServerSession.new(), 21, opts)
    :ok = Session.start_transaction(session)
    {:ok, _conn, _cmd, _opts} = Session.bind_session(session, [insert: "events"], [])
    session
  end
end
