defmodule Crisp.ClientTest do
  use ExUnit.Case
  alias Crisp.{Client, Job, Queue, Server}

  import Crisp.Utilities

  describe "start" do
    test "start helper" do
      assert :ok == Client.start()
    end
  end

  describe "simple client queue interaction" do
    setup do
      Server.start()
      Client.start()

      qname = "client-test-#{time_usecs()}"
      queue = Queue.new(qname, runlimit: 10)
      {{:ok, _}, queue} = Server.create_queue(queue)

      on_exit(fn ->
        Server.start()
        :timer.sleep(250)
        Server.delete_queue(queue)
      end)

      %{queue: queue}
    end

    test "queue job", %{queue: queue} do
      job = Job.new("test_job")
      {queued_status, _} = Client.queue_job(queue.name, job)
      assert queued_status == :ok
    end

    test "queue job in subqueue", %{queue: queue} do
      job = Job.new("test_job")
      {queued_status, _} = Client.queue_job(queue.name, job, :runnable)
      assert queued_status == :ok
    end

    test "fetch newer jobs", %{queue: queue} do
      max = 2

      for i <- 1..max do
        Client.queue_job(queue.name, Job.new("test_fetch_#{i}"), :runnable)
      end

      {status, jobs} = Client.fetch_newer_jobs(queue.name, :runnable, 0, 100)
      assert status == :ok
      assert length(jobs) == max * 2
      # Last 'score'
      last_usecs = List.last(jobs)

      for i <- 1..max do
        Client.queue_job(
          queue.name,
          Job.new("test_fetch_later_#{i}"),
          :runnable
        )
      end

      {new_status, new_jobs} =
        Client.fetch_newer_jobs(queue.name, :runnable, last_usecs, 100)

      assert new_status == :ok
      assert length(new_jobs) == max * 2
    end
  end
end
