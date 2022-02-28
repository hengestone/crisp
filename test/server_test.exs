defmodule Crisp.ServerTest do
  use ExUnit.Case
  alias Crisp.{Client, Job, Server, Utilities}

  describe "start" do
    test "start helper" do
      assert :ok == Server.start()
    end
  end

  describe "queue CRUD" do
    setup do
      Server.start()
    end

    test "create_queue" do
      {{status, _}, _queue} = Server.create_queue("testq")
      assert status == :ok
    end

    test "delete_queue" do
      {{status, _}, queue} = Server.create_queue("testq")
      assert status == :ok
      {{new_status, _}, _deleted_queue} = Server.delete_queue(queue)
      assert new_status == :ok
    end
  end

  describe "queue info" do
    setup do
      Server.start()
    end

    test "get local queues" do
      queues = Server.queues()
      assert is_map(queues)
    end

    test "get remote queues" do
      {result, queues} = Server.remote_queues()
      assert result == :ok
      assert is_list(queues)
    end

    test "update local queues" do
      result = Server.update_local()
      assert result == :ok
    end

    test "queue_counts" do
      {{status, _}, queue} = Server.create_queue("testq")
      {count_status, %Crisp.QueueCounts{}} = Server.queue_counts(queue)
      assert status == :ok
      assert count_status == :ok
    end

    test "queue_counts string name" do
      {{status, _}, _queue} = Server.create_queue("testq")
      {count_status, %Crisp.QueueCounts{}} = Server.queue_counts("testq")
      assert status == :ok
      assert count_status == :ok
    end
  end

  describe "queue runnable" do
    setup do
      Server.start()
      Client.start()
      qname = Utilities.utc_id()
      {{:ok, _}, queue} = Server.create_queue(qname)

      on_exit(fn ->
        Server.start()
        Server.delete_queue(queue)
      end)

      %{queue: queue}
    end

    test "queue runnable, under limit", %{queue: queue} do
      numjobs = 10

      for i <- 1..numjobs do
        Client.queue_job(queue, Job.new("job_#{i}"))
      end

      {status, value} = Server.queue_runnable(queue)
      assert status == :ok
      {_counts, retval} = value
      assert is_list(retval)
      assert length(retval) == numjobs * 2
    end
  end
end
