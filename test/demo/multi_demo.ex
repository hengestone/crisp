defmodule Crisp.MultiDemo do
  alias Crisp.{
    Client,
    Job,
    MultiScheduler,
    Queue,
    Server,
    WorkerSupervisor
  }

  def queue_jobs(
        %{queue: _queue, spec: _spec, worker_queues: worker_queues},
        num
      ) do
    for i <- 1..num do
      worker_name = Map.keys(worker_queues) |> Enum.random()
      worker_queue = Map.get(worker_queues, worker_name)

      {:ok, _id} =
        Client.queue_job(
          worker_queue,
          Job.new("multi_queue_job_#{i}", node: "provider-site"),
          :queued
        )
    end

    :ok
  end

  def teardown(:queue_jobs, %{worker_queues: worker_queues}) do
    Server.start()

    Enum.each(worker_queues, fn {_, queue} ->
      Process.sleep(50)
      Server.delete_queue(queue)
    end)
  end

  def setup() do
    Server.start()
    qname = "aws-transcribe"
    queue = Queue.new(qname, runlimit: 10)
    {{:ok, _}, queue} = Server.create_queue(queue)

    worker_queues =
      Enum.map(1..5, fn i ->
        worker_name = "worker-#{i}"
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
      MultiScheduler.start(queue_name: qname, queue_pattern: "worker-*")

    %{queue: queue, pid: pid, spec: spec, worker_queues: worker_queues}
  end

  def register_jobs(%{spec: spec, worker_queues: worker_queues}) do
    Enum.each(worker_queues, fn {_, queue} ->
      {:ok, pid} =
        WorkerSupervisor.start(
          queue_name: queue.name,
          poll_delay: 1000,
          run_delay: 1000
        )

      WorkerSupervisor.register_worker(pid, spec.module, spec.max, %Job{
        version: Job.version(),
        node: "provider-site"
      })
    end)
  end

  def schedule_jobs_cycle(%{spec: _spec, worker_queues: worker_queues}) do
    Enum.each(worker_queues, fn {_, queue} ->
      Server.queue_runnable(queue)
    end)
  end
end
