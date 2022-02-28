defmodule Crisp.MultiSchedulerTest do
  use ExUnit.Case
  import Crisp.Utilities

  alias Crisp.{
    Client,
    Job,
    MultiScheduler,
    Queue,
    Server
  }

  describe "start" do
    setup do
      Process.sleep(250)
      Server.start()
      Process.sleep(250)

      {{:ok, _}, queue} = Server.create_queue("aws-transcribe-test")

      on_exit(fn ->
        Server.start()
        Process.sleep(250)
        Server.delete_queue(queue)
        Process.sleep(250)
      end)

      :ok
    end

    test "start helper" do
      {:ok, pid} =
        MultiScheduler.start(
          queue_name: "aws-transcribe-test",
          queue_pattern: "transcribe-worker-test*"
        )

      assert is_pid(pid)
    end
  end

  describe "queuing" do
    setup do
      Process.sleep(100)
      Server.start()
      Process.sleep(100)
      Client.start()
      test_id = mt_id()
      qname = "aws-#{test_id}"
      queue = Queue.new(qname, runlimit: 10)
      {{:ok, _}, queue} = Server.create_queue(queue)

      worker_queues =
        Enum.map(1..5, fn i ->
          worker_name = "tsworker-#{test_id}-#{i}"
          worker_queue = Queue.new(worker_name, runlimit: 10)
          {{:ok, _}, worker_queue} = Server.create_queue(worker_queue)
          {worker_name, worker_queue}
        end)
        |> Map.new()

      spec = %{
        module: Crisp.TestWorker,
        max: 5
      }

      {:ok, pid} =
        MultiScheduler.start(
          count_delay: 0,
          queue_name: qname,
          queue_pattern: "tsworker-#{test_id}-*",
          run_delay: 50
        )

      on_exit(fn ->
        Server.start()
        Process.sleep(250)
        Server.delete_queue(queue)

        for {_name, wqueue} <- worker_queues do
          Server.delete_queue(wqueue)
        end

        Process.sleep(250)
      end)

      %{
        queue: queue,
        pid: pid,
        spec: spec,
        worker_queues: worker_queues,
        test_id: test_id
      }
    end

    test "one job", %{
      queue: queue,
      pid: pid,
      worker_queues: worker_queues,
      test_id: _test_id
    } do
      worker_queue = worker_queues |> Map.values() |> Enum.random()
      job = Job.new("tsworker-one-job", node: "tsworker-one-node")

      Client.queue_job(worker_queue, job, :runnable)

      Process.sleep(1000)
      state = MultiScheduler.state(pid)
      assert state.slots_avail == queue.runlimit - 1
      {:ok, qcounts} = Server.queue_counts(state.queue_name)
      {:ok, _names, counts} = MultiScheduler.worker_counts(state)
      assert qcounts.runnable == 1
      assert counts == [0, 0, 0, 0, 0]
    end

    test "many jobs", %{
      queue: queue,
      pid: pid,
      worker_queues: worker_queues,
      test_id: _test_id
    } do
      for {qname, worker_queue} <- worker_queues do
        job = Job.new("tsworker-one-job", node: "tsworker-one-#{qname}")

        Client.queue_job(worker_queue, job, :runnable)
      end

      Process.sleep(500)
      state = MultiScheduler.state(pid)
      assert state.slots_avail == queue.runlimit - map_size(worker_queues)
      {:ok, qcounts} = Server.queue_counts(state.queue_name)
      {:ok, _names, counts} = MultiScheduler.worker_counts(state)
      assert qcounts.runnable == map_size(worker_queues)
      assert counts == [0, 0, 0, 0, 0]
    end
  end
end
