defmodule Crisp.Demo do
  alias Crisp.{Client, Job, Queue, Server, WorkerSupervisor, TestWorker}

  import Crisp.Utilities

  @spec queue_jobs(integer) :: %{
          queue: Crisp.Queue.t(),
          spec: map
        }
  def queue_jobs(num) when is_integer(num) do
    %{queue: queue, spec: spec} = queue_jobs_setup()

    for i <- 1..num do
      {:ok, _id} =
        Client.queue_job(
          queue,
          Job.new("demo_queue_job_#{i}", node: "provider-site"),
          :queued
        )
    end

    %{queue: queue, spec: spec}
  end

  @spec queue_jobs(%{queue: Crisp.Queue.t(), spec: map}, integer) :: :ok
  def queue_jobs(%{queue: queue, spec: _spec}, num) do
    for i <- 1..num do
      {:ok, _id} =
        Client.queue_job(
          queue,
          Job.new("demo_queue_job_#{i}", node: "provider-site"),
          :queued
        )
    end

    :ok
  end

  @spec runnable_jobs(%{queue: binary | Crisp.Queue.t()}) :: any
  def runnable_jobs(status) do
    Server.queue_runnable(status.queue)
  end

  @spec register_jobs(%{
          queue: Crisp.Queue.t(),
          spec: %{max: integer, module: atom}
        }) :: :ok
  def register_jobs(%{spec: spec, queue: queue}) do
    WorkerSupervisor.start(
      queue_name: queue.name,
      poll_delay: 1000,
      run_delay: 1000
    )

    WorkerSupervisor.register_worker(spec.module, spec.max, %Job{
      version: Job.version(),
      node: "provider-site"
    })
  end

  def runnable_jobs_cycle(%{spec: _spec, queue: _queue} = _status) do
    WorkerSupervisor.poll_runnable_queue()
    WorkerSupervisor.start_runnable()
  end

  def schedule_jobs_cycle(%{spec: _spec, queue: queue} = status) do
    finish_jobs(status)
    Process.sleep(50)
    Server.queue_runnable(queue)
    Process.sleep(50)
    WorkerSupervisor.poll_runnable_queue()
    WorkerSupervisor.start_runnable()
  end


  @spec count_jobs(%{queue: Crisp.Queue.t()}) :: any
  def count_jobs(status) do
    Server.queue_counts(status.queue)
  end

  def finish_jobs(_status) do
    state = WorkerSupervisor.state()

    for {pid, _spec} <- state.running_pids do
      TestWorker.exit(pid, :normal)
    end
  end

  @spec teardown(:queue_jobs, %{queue: Crisp.Queue.t()}) :: any
  def teardown(:queue_jobs, status) do
    WorkerSupervisor.stop()
    Server.start()
    Process.sleep(50)
    Server.delete_queue(status.queue)
  end

  defp queue_jobs_setup() do
    Server.start()
    qname = "demo-#{time_usecs()}"
    queue = Queue.new(qname, runlimit: 10)
    {{:ok, _}, queue} = Server.create_queue(queue)

    spec = %{
      module: Crisp.TestWorker,
      max: 5
    }

    %{queue: queue, spec: spec}
  end
end
