defmodule Mongo.StreamingMonitorWaitTest do
  use ExUnit.Case, async: false

  alias Mongo.Events.MoreToComeEvent

  @samples 30
  # The streaming monitor waits 10 s (maxAwaitTimeMS) for the first awaitable hello, then waits on moreToCome replies.
  @first_await_ms 10_500

  # A purge of the Mongo module (Patch mocks) kills every process that runs its old code, so the monitor must not wait in it.
  @tag :rs_required
  @tag timeout: 30_000
  test "the streaming monitor waits for hello and moreToCome replies outside the Mongo module" do
    # The default depth of 8 frames cuts the stack below the caller of DBConnection.execute/4.
    depth = :erlang.system_flag(:backtrace_depth, 64)
    on_exit(fn -> :erlang.system_flag(:backtrace_depth, depth) end)
    {:ok, _} = Registry.register(:events_registry, :commands, [])

    top = start_supervised!({Mongo, database: "mongodb_test", seeds: ["127.0.0.1:27017"], name: :streaming_monitor_wait_test})
    {:ok, _} = Mongo.ping(top)
    pid = streaming_pid(top)

    hello_stacks = sample_stacks(pid, @samples)
    pause(@first_await_ms)
    assert_received {:broadcast, :commands, %MoreToComeEvent{}}
    more_to_come_stacks = sample_stacks(pid, @samples)

    for stacks <- [hello_stacks, more_to_come_stacks] do
      assert Enum.any?(stacks, &waits_in_db_connection?/1)
      refute Enum.any?(stacks, &runs_mongo_module?/1)
    end
  end

  defp streaming_pid(top, attempts \\ 50) do
    pids =
      for {_address, monitor} <- :sys.get_state(top).monitors,
          pid = :sys.get_state(monitor).streaming_pid,
          is_pid(pid),
          do: pid

    case {pids, attempts} do
      {[pid | _], _} ->
        pid

      {[], 0} ->
        flunk("no streaming monitor started")

      {[], _} ->
        pause(50)
        streaming_pid(top, attempts - 1)
    end
  end

  defp sample_stacks(_pid, 0), do: []

  defp sample_stacks(pid, samples) do
    {:current_stacktrace, stack} = Process.info(pid, :current_stacktrace)
    pause(50)
    [stack | sample_stacks(pid, samples - 1)]
  end

  defp pause(ms) do
    receive do
    after
      ms -> :ok
    end
  end

  defp waits_in_db_connection?(stack), do: Enum.any?(stack, fn {module, _fun, _arity, _location} -> String.starts_with?(inspect(module), "DBConnection") end)

  defp runs_mongo_module?(stack), do: Enum.any?(stack, fn {module, _fun, _arity, _location} -> module == Mongo end)
end
