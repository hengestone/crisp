# Crisp
Central Resource Protector.

## What

**A flexible Elixir Workqueue and Scheduler implementation**

## Why

Using a REST service we faced an issue of having to:

 - Respect the maximum number of concurrent jobs allowed _N_
 - Keep the request rate under a maximum of _M_
 - Give configurable priority to jobs from different providers
 - Keep throughput and latency reasonable for all types of jobs
 - Different jobs types are run on independent nodes.

This is of course a classic queueing/scheduling problem with global resource constraints. Crisp acts as a global resource coordinator.

## Other queueing systems

  - [Exq](https://github.com/akira/exq) A Redis-based queue implementation compatible with ResQueue.
  - [Toniq](https://github.com/joakimk/toniq) A Redis-based queue implementation
  - [Kiq](https://github.com/sorentwo/kiq) Contains an extensive comparison of queueing systems.
  - [Oban](https://github.com/sorentwo/oban) A PostgreSQL based job processor. 
  - And a raft of others on [GitHub](https://github.com/search?q=elixir+queue)

## How
  The primary Redis datastructure used by **Crisp** is a [Sorted set](https://redis.io/topics/data-types) with a _score_ equal to a millisecond timestamp, not a List. This makes it **trivial** to implement a work queue where
  
  - jobs can be fetched from any part of the structure based on age and a search pattern.
  - jobs can be aged out
  - the size of the queue can be determined efficiently

While it is certainly possible to retrofit this onto other queueing systems using _middleware_ and unique queues for different job types, and implementing scheduler that reassigns jobs onto a central contrained job queue it was felt that a simpler, but more flexible approach would be more optimal.

## Design decisions

  - Use Redis for persistent queue state and publish subscribe.
  - Encode data structures in the queue as CSV text for easy search.

## Installation

The package can be installed
by adding `crisp` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:crisp,
       git: "https://github.com/hengestone/crisp.git", branch: "master"},
    {:redix, ">= 0.0.0"},
    {:castore, ">= 0.0.0"}
  ]
end
```

## Usage
There are two main parts to the implementation that need to run: a Scheduler/server and one or more Worker nodes. 

### Configuration
In `config.exs` or environment-specific configuration add the Redis server endpoint according to the [Redix](https://github.com/whatyouhide/redix) documentation:
```elixir
config :crisp, :redis,
  host: "localhost",
  port: 6379,
  name: :crisp
```

Similarly, add the Redis namespace to use, e.g.:
```elixir
config :crisp,
  namespace: "crisp_#{Mix.env()}"
```

### Server
To create one or more queues do _once_:
```elixir
alias Crisp.Server
Server.start()
Server.create_queue("main-runqueue", runlimit: 100)
Server.create_queue("worker-queue-1")
Server.create_queue("worker-queue-2")
```

This will create a main queue with a concurrency limit of **100**
Subsequently, only the sceheduler needs to be started:
```elixir
alias Crisp.MultiSCheduler
MultiScheduler.start(
    queue_name: "main-runqueue",
    queue_pattern: "worker-queue-*"
  )
```

The scheduler is a _GenServer_ that can be started by a supervisor as well:
```elixir
def start(_type, _args) do
  children =
    [
      worker(
        Crisp.MultiScheduler,
        [
          queue_name: "main-runqueue",
          queue_pattern: "worker-queue-*"
        ],
      )
    ] 

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]
  Supervisor.start_link(children, opts)
end
```
### Worker Node
Start the worker supervisor on the node:
```elixir
Crisp.WorkerSupervisor.start(queue_name: "main-runqueue")
```

### Job submission
Start the `Client` and submit jobs:
```elixir
alias Crisp.{Client, Job, Queue}
Client.start()

queue = Queue.new("worker-queue-1")
job = Job.new("worker-job-name", node: "worker-node-1")

{:ok, job_id} = Client.queue_job(queue, job, :runnable)
```

### Workers

Workers are _GenServer_ instances that conform to the ``JobWorkerBehaviour``:
```elixir
@callback start(Crisp.Job.t()) :: :ignore | {:error, any} | {:ok, pid}
@callback id(pid) :: {:ok, binary}
```

I.e. workers take a `Job` instance as a `start()` parameter and must provide an `id(pid)` lookup function.

An example worker looks as follows:
```elixir
defmodule Crisp.TestWorker do
  @behaviour Crisp.JobWorkerBehaviour

  alias Crisp.Job

  use GenServer, restart: :transient

  @impl true
  @spec start(Crisp.Job.t()) :: :ignore | {:error, any} | {:ok, pid}
  def start(%Job{} = job) do
    GenServer.start(__MODULE__, %{job: job})
  end

  @impl true
  @spec init(any) :: {:ok, any}
  def init(state) do
    {:ok, state}
  end

  @impl true
  @spec id(pid) :: {:ok, binary}
  def id(pid) do
    GenServer.call(pid, :id)
  end

  def exit(pid, reason) do
    GenServer.cast(pid, {:exit, reason})
  end

  # Server part ----------------------------------------------------------------
  @impl true
  @spec handle_call(:id, {pid, any}, map) :: {:reply, {:ok, binary}, map}
  def handle_call(:id, _from, %{job: job} = state) do
    {:reply, {:ok, job.id}, state}
  end

  @impl true
  @spec handle_cast({:exit, any}, map) :: {:stop, any, map}
  def handle_cast({:exit, reason}, state) do
    {:stop, reason, state}
  end
end
```

### Worker registration
The last piece of the puzzle is to tell the `WorkerSupervisor` which kinds of jobs it will oversee as well as the maximum concurrency:

```elixir
alias Crisp.{job, TestWorker, WorkerSupervisor}

WorkerSupervisor.register_worker(TestWorker, max_concurrency, %Job{
  version: Job.version(),
  node: "worker-node-1"
})
```

## Summary
In the above an example we created 2 work queues that are used by the scheduler to feed into the `main-runqueue`. The scheduler uses random sampling to take jobs from the work queues, controlled by the `MultiScheduler.pick_random()` function.

## Tests
ExUnit tests can be run using `mix test`.
