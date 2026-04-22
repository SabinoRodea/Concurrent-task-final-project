# Concurrent Task Dispatcher in Rust

This project is a Rust simulation of a concurrent task dispatcher. It models a stream of incoming CPU-bound and I/O-bound tasks, places them into queue-based scheduling structures, and assigns them to a fixed-size worker pool. The program tracks timing and queue behavior so different scheduling approaches can be compared under balanced and stressed workloads.

## Overview

The goal of the project is to show how a scheduler can be built from smaller concurrent parts:

- a task generator that produces work over time
- a dispatcher that decides what should run next
- a bounded worker pool that executes tasks
- a monitor that samples system activity while the run is in progress
- a statistics collector that summarizes the final results

The system is a simulation rather than a real operating-system scheduler, but it still reflects the same core ideas: queuing, resource limits, fairness, throughput, and clean shutdown.

## Features

- 1000 automatically generated tasks per run
- fixed random seed for reproducible results
- fixed-size worker pool with 10 workers
- separate concurrency roles for generator, dispatcher, workers, and monitor
- queue-based scheduling architecture
- clean shutdown logic
- two scheduling strategies for comparison:
  - **FIFO** using a single shared ready queue
  - **Optimized** using separate CPU and I/O queues with a simple reservation policy
- simulated global CPU cap of 100%

## Metrics Collected

The program prints summary statistics for each run, including:

- total tasks completed
- makespan
- average wait time
- average turnaround time
- average simulated CPU usage
- average busy workers
- average queue length
- maximum queue length
- worker utilization
- fairness gap between CPU and I/O wait times

## Project Structure

```text
concurrent_task_dispatcher/
├── Cargo.toml
├── Cargo.lock
├── README.md
├── REPORT.md
├── report.pdf
├── experiment_output.txt
└── src/
    └── main.rs
```

## Building the Project

Make sure Rust and Cargo are installed, then open a terminal in the project folder and run:

```bash
cargo build
```

## Running the Project

To run the simulation:

```bash
cargo run --release
```

The program runs four configurations automatically:

1. balanced workload with FIFO
2. balanced workload with optimized scheduling
3. stressed workload with FIFO
4. stressed workload with optimized scheduling

## Useful Commands

```bash
cargo build
cargo run
cargo run --release
cargo fmt
cargo clippy
```

## Design Summary

### Major Components

**Generator thread**  
Creates tasks over time and sends them into the system.

**Dispatcher logic**  
Receives incoming tasks, places them into the appropriate queue, checks worker availability, checks the current CPU budget, and decides what task to send next.

**Worker pool**  
Ten worker threads wait for assigned work, simulate task execution with `sleep`, and report back when they become available again.

**Monitor thread**  
Samples queue length, worker activity, and CPU usage every 10 milliseconds during the run.

**Main thread**  
Starts each experiment, waits for all threads to finish, and prints the final results.

### Shared State

`Arc<Mutex<SharedState>>` is used to protect data that multiple threads need to read or update, including:

- total queue length
- CPU queue length
- I/O queue length
- busy worker count
- current CPU load
- completion flag

`Arc<Mutex<CollectorState>>` stores:

- simulation start time
- per-worker busy time
- completed task records

### Channel Usage

Channels are used to move ownership of work between components:

- generator -> dispatcher for arriving tasks
- workers -> dispatcher for worker availability notifications
- dispatcher -> each worker through a dedicated bounded channel

This keeps task flow message-based and avoids putting the entire system behind one oversized shared lock.

## Scheduling Policies

### FIFO

FIFO uses one shared ready queue. The dispatcher always tries to send the task at the front of the queue first, as long as a worker is available and the global CPU budget would not be exceeded.

This approach is simple and easy to explain, but it can suffer from head-of-line blocking when a heavier task at the front delays lighter work behind it.

### Optimized Policy

The optimized version uses two queues:

- one CPU queue
- one I/O queue

Policy behavior:

- two workers are effectively reserved for CPU tasks first
- the remaining workers prefer I/O tasks first, then CPU tasks
- if the first choice does not fit within the remaining CPU budget, the dispatcher tries the other queue

This policy is meant to keep lighter I/O work moving even when CPU-heavy jobs arrive more aggressively.

## Experiments

### Experiment A: Balanced Workload

- 1000 tasks
- roughly 70% I/O and 30% CPU
- arrivals centered around 20 ms
- CPU tasks consume 40% simulated CPU budget
- I/O tasks consume 10% simulated CPU budget
- durations vary around 180 to 220 ms

### Experiment B: Stressed Workload

- 1000 tasks
- CPU tasks are more common
- arrivals are more bursty
- CPU tasks are longer on average
- the same 100% CPU budget is still enforced

## Interpreting the Results

Under the balanced workload, FIFO usually performs reasonably well because the task mix is not too extreme. Under the stressed workload, the optimized scheduler should usually perform better because it prevents CPU-heavy work from dominating the entire system and gives lighter I/O work more chances to move through the system.

## Tool Use Disclosure

I used outside references while building and polishing this project, mainly to double-check Rust concurrency patterns and to improve the clarity of the written explanation. The help I accepted was advice on how to structure the project cleanly around a generator, dispatcher, worker pool, and monitor, along with suggestions on what metrics would be most useful to report. One suggestion I had to adjust was making the design too complicated too early with extra scheduling features. I kept the final version simpler so it would be easier to explain, debug, and defend during a demo.

## Notes

The design was intentionally kept simple enough to explain during a live demo while still meeting the project requirements for concurrency, queue-based dispatching, metrics, experiments, and clean shutdown.
