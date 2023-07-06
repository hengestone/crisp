defmodule Crisp.MultiDemo do
  alias Crisp.{
    Client,
    Job,
    MultiScheduler,
    Queue,
    Server
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
          :runnable
        )
    end

    :ok
  end

  @spec teardown(:queue_jobs, %{queue: Crisp.Queue.t()}) :: any
  def teardown(:queue_jobs, status) do
    Server.start()
    Process.sleep(50)
    Server.delete_queue(status.queue)
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
end
