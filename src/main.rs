use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const WORKER_COUNT: usize = 10;
const MONITOR_INTERVAL_MS: u64 = 10;
const GENERATOR_JITTER_MS: u64 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TaskKind {
    Cpu,
    Io,
}

impl fmt::Display for TaskKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskKind::Cpu => write!(f, "CPU"),
            TaskKind::Io => write!(f, "IO"),
        }
    }
}

#[derive(Clone, Debug)]
struct Task {
    id: usize,
    arrival_offset: Duration,
    kind: TaskKind,
    duration: Duration,
    cpu_cost: f64,
}

#[derive(Clone, Copy, Debug)]
enum Policy {
    Fifo,
    Optimized,
}

impl fmt::Display for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Policy::Fifo => write!(f, "FIFO"),
            Policy::Optimized => write!(f, "Optimized"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum WorkloadKind {
    Balanced,
    Stressed,
}

impl fmt::Display for WorkloadKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkloadKind::Balanced => write!(f, "Balanced"),
            WorkloadKind::Stressed => write!(f, "Stressed"),
        }
    }
}

#[derive(Clone, Debug)]
struct ExperimentConfig {
    name: &'static str,
    workload: WorkloadKind,
    policy: Policy,
    task_count: usize,
    base_interval_ms: u64,
    worker_count: usize,
    seed: u64,
}

#[derive(Clone, Copy, Debug)]
struct TaskRecord {
    task_id: usize,
    worker_id: usize,
    kind: TaskKind,
    wait_time: Duration,
    turnaround_time: Duration,
    cpu_cost: f64,
}

#[derive(Debug)]
struct MonitorSample {
    elapsed: Duration,
    queue_len: usize,
    cpu_queue_len: usize,
    io_queue_len: usize,
    busy_workers: usize,
    cpu_load: f64,
}

#[derive(Debug)]
struct SharedState {
    total_queue_len: usize,
    cpu_queue_len: usize,
    io_queue_len: usize,
    busy_workers: usize,
    current_cpu_load: f64,
    done: bool,
}

impl SharedState {
    fn new() -> Self {
        Self {
            total_queue_len: 0,
            cpu_queue_len: 0,
            io_queue_len: 0,
            busy_workers: 0,
            current_cpu_load: 0.0,
            done: false,
        }
    }
}

#[derive(Debug)]
struct RunSummary {
    config: ExperimentConfig,
    total_tasks_completed: usize,
    makespan: Duration,
    average_wait: Duration,
    average_turnaround: Duration,
    max_wait: Duration,
    average_cpu_usage: f64,
    average_busy_workers: f64,
    max_queue_len: usize,
    average_queue_len: f64,
    cpu_tasks_completed: usize,
    io_tasks_completed: usize,
    worker_utilization: Vec<f64>,
    fairness_gap_ms: f64,
}

#[derive(Debug)]
struct CollectorState {
    run_start: Instant,
    worker_busy_time: Vec<Duration>,
    records: Vec<TaskRecord>,
}

impl CollectorState {
    fn new(worker_count: usize) -> Self {
        Self {
            run_start: Instant::now(),
            worker_busy_time: vec![Duration::ZERO; worker_count],
            records: Vec::new(),
        }
    }
}

#[derive(Debug)]
enum WorkerMessage {
    Run(Task),
    Shutdown,
}

fn main() {
    let experiments = vec![
        ExperimentConfig {
            name: "balanced_fifo",
            workload: WorkloadKind::Balanced,
            policy: Policy::Fifo,
            task_count: 1000,
            base_interval_ms: 20,
            worker_count: WORKER_COUNT,
            seed: 7,
        },
        ExperimentConfig {
            name: "balanced_optimized",
            workload: WorkloadKind::Balanced,
            policy: Policy::Optimized,
            task_count: 1000,
            base_interval_ms: 20,
            worker_count: WORKER_COUNT,
            seed: 7,
        },
        ExperimentConfig {
            name: "stressed_fifo",
            workload: WorkloadKind::Stressed,
            policy: Policy::Fifo,
            task_count: 1000,
            base_interval_ms: 12,
            worker_count: WORKER_COUNT,
            seed: 77,
        },
        ExperimentConfig {
            name: "stressed_optimized",
            workload: WorkloadKind::Stressed,
            policy: Policy::Optimized,
            task_count: 1000,
            base_interval_ms: 12,
            worker_count: WORKER_COUNT,
            seed: 77,
        },
    ];

    let mut results = Vec::new();

    for config in experiments {
        println!("\n============================================================");
        println!(
            "Running {} workload with {} policy ({} tasks, {} workers)",
            config.workload, config.policy, config.task_count, config.worker_count
        );
        println!("============================================================");
        let summary = run_simulation(config.clone());
        print_summary(&summary);
        results.push(summary);
    }

    print_comparison(&results);
}

fn run_simulation(config: ExperimentConfig) -> RunSummary {
    let tasks = generate_tasks(&config);
    let shared_state = Arc::new(Mutex::new(SharedState::new()));
    let collector_state = Arc::new(Mutex::new(CollectorState::new(config.worker_count)));

    let (task_tx, task_rx) = mpsc::channel::<Task>();
    let (ready_tx, ready_rx) = mpsc::channel::<usize>();

    let mut worker_senders = Vec::new();
    let mut worker_handles = Vec::new();

    for worker_id in 0..config.worker_count {
        let (worker_tx, worker_rx) = mpsc::sync_channel::<WorkerMessage>(1);
        worker_senders.push(worker_tx);

        let state = Arc::clone(&shared_state);
        let collector = Arc::clone(&collector_state);
        let ready = ready_tx.clone();

        let handle = thread::spawn(move || worker_loop(worker_id, worker_rx, ready, state, collector));
        worker_handles.push(handle);
    }
    drop(ready_tx);

    let monitor_state = Arc::clone(&shared_state);
    let run_start = {
        let collector = collector_state.lock().unwrap_or_else(|poison| poison.into_inner());
        collector.run_start
    };
    let monitor_handle = thread::spawn(move || monitor_loop(monitor_state, run_start));

    let generator_handle = thread::spawn(move || generator_loop(tasks, task_tx, run_start, config.base_interval_ms));

    dispatcher_loop(&config, task_rx, ready_rx, &worker_senders, Arc::clone(&shared_state));

    generator_handle.join().expect("generator thread panicked");

    {
        let mut state = shared_state.lock().unwrap_or_else(|poison| poison.into_inner());
        state.done = true;
    }

    let monitor_samples = monitor_handle.join().expect("monitor thread panicked");

    for handle in worker_handles {
        handle.join().expect("worker thread panicked");
    }

    let collector = collector_state.lock().unwrap_or_else(|poison| poison.into_inner());
    summarize_run(config, &collector, &monitor_samples)
}

fn generator_loop(tasks: Vec<Task>, task_tx: mpsc::Sender<Task>, run_start: Instant, base_interval_ms: u64) {
    let mut rng = SmallRng::seed_from_u64(9999 + base_interval_ms);

    for task in tasks {
        let target = run_start + task.arrival_offset;
        let now = Instant::now();
        if target > now {
            thread::sleep(target.duration_since(now));
        }

        if task_tx.send(task).is_err() {
            break;
        }

        let jitter = rng.gen_range(0..=GENERATOR_JITTER_MS);
        if jitter > 0 {
            thread::sleep(Duration::from_millis(jitter));
        }
    }
}

fn dispatcher_loop(
    config: &ExperimentConfig,
    task_rx: Receiver<Task>,
    ready_rx: Receiver<usize>,
    worker_senders: &[SyncSender<WorkerMessage>],
    shared_state: Arc<Mutex<SharedState>>,
) {
    let mut generator_done = false;
    let mut available_workers = VecDeque::new();
    let mut fifo_queue = VecDeque::new();
    let mut cpu_queue = VecDeque::new();
    let mut io_queue = VecDeque::new();

    let cpu_reserved_workers = match config.policy {
        Policy::Fifo => 0,
        Policy::Optimized => 2,
    };

    let mut prefer_io = true;

    loop {
        loop {
            match ready_rx.try_recv() {
                Ok(worker_id) => available_workers.push_back(worker_id),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }

        loop {
            match task_rx.try_recv() {
                Ok(task) => match config.policy {
                    Policy::Fifo => fifo_queue.push_back(task),
                    Policy::Optimized => match task.kind {
                        TaskKind::Cpu => cpu_queue.push_back(task),
                        TaskKind::Io => io_queue.push_back(task),
                    },
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    generator_done = true;
                    break;
                }
            }
        }

        update_queue_state(&shared_state, &fifo_queue, &cpu_queue, &io_queue);

        while let Some(worker_id) = available_workers.pop_front() {
            let current_cpu_load = {
                let state = shared_state.lock().unwrap_or_else(|poison| poison.into_inner());
                state.current_cpu_load
            };

            let maybe_task = match config.policy {
                Policy::Fifo => pop_fifo_task(&mut fifo_queue, current_cpu_load),
                Policy::Optimized => pop_optimized_task(
                    worker_id,
                    config.worker_count,
                    cpu_reserved_workers,
                    &mut cpu_queue,
                    &mut io_queue,
                    current_cpu_load,
                    &mut prefer_io,
                ),
            };

            if let Some(task) = maybe_task {
                {
                    let mut state = shared_state.lock().unwrap_or_else(|poison| poison.into_inner());
                    state.current_cpu_load = (state.current_cpu_load + task.cpu_cost).min(1.0);
                    state.busy_workers += 1;
                }
                update_queue_state(&shared_state, &fifo_queue, &cpu_queue, &io_queue);
                let _ = worker_senders[worker_id].send(WorkerMessage::Run(task));
            } else {
                available_workers.push_front(worker_id);
                break;
            }
        }

        let queues_empty = fifo_queue.is_empty() && cpu_queue.is_empty() && io_queue.is_empty();
        if generator_done && queues_empty && available_workers.len() == config.worker_count {
            break;
        }

        if !generator_done || !queues_empty {
            match task_rx.recv_timeout(Duration::from_millis(2)) {
                Ok(task) => match config.policy {
                    Policy::Fifo => fifo_queue.push_back(task),
                    Policy::Optimized => match task.kind {
                        TaskKind::Cpu => cpu_queue.push_back(task),
                        TaskKind::Io => io_queue.push_back(task),
                    },
                },
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    generator_done = true;
                }
            }
        }

        update_queue_state(&shared_state, &fifo_queue, &cpu_queue, &io_queue);
    }

    for sender in worker_senders {
        let _ = sender.send(WorkerMessage::Shutdown);
    }
}

fn pop_fifo_task(queue: &mut VecDeque<Task>, current_cpu_load: f64) -> Option<Task> {
    match queue.front() {
        Some(task) if current_cpu_load + task.cpu_cost <= 1.0 => queue.pop_front(),
        _ => None,
    }
}

fn pop_optimized_task(
    worker_id: usize,
    worker_count: usize,
    cpu_reserved_workers: usize,
    cpu_queue: &mut VecDeque<Task>,
    io_queue: &mut VecDeque<Task>,
    current_cpu_load: f64,
    prefer_io: &mut bool,
) -> Option<Task> {
    let reserved_start = worker_count.saturating_sub(cpu_reserved_workers);
    let cpu_reserved = worker_id >= reserved_start;

    let first_kind = if cpu_reserved {
        TaskKind::Cpu
    } else if *prefer_io {
        TaskKind::Io
    } else {
        TaskKind::Cpu
    };

    let second_kind = match first_kind {
        TaskKind::Cpu => TaskKind::Io,
        TaskKind::Io => TaskKind::Cpu,
    };

    if let Some(task) = pop_kind(first_kind, cpu_queue, io_queue, current_cpu_load) {
        *prefer_io = !*prefer_io;
        return Some(task);
    }

    if let Some(task) = pop_kind(second_kind, cpu_queue, io_queue, current_cpu_load) {
        *prefer_io = !*prefer_io;
        return Some(task);
    }

    None
}

fn pop_kind(
    kind: TaskKind,
    cpu_queue: &mut VecDeque<Task>,
    io_queue: &mut VecDeque<Task>,
    current_cpu_load: f64,
) -> Option<Task> {
    let queue = match kind {
        TaskKind::Cpu => cpu_queue,
        TaskKind::Io => io_queue,
    };

    match queue.front() {
        Some(task) if current_cpu_load + task.cpu_cost <= 1.0 => queue.pop_front(),
        _ => None,
    }
}

fn worker_loop(
    worker_id: usize,
    worker_rx: Receiver<WorkerMessage>,
    ready_tx: mpsc::Sender<usize>,
    shared_state: Arc<Mutex<SharedState>>,
    collector_state: Arc<Mutex<CollectorState>>,
) {
    let _ = ready_tx.send(worker_id);

    while let Ok(message) = worker_rx.recv() {
        match message {
            WorkerMessage::Run(task) => {
                let now = Instant::now();
                let dispatch_delay = now.saturating_duration_since(task_origin(task.arrival_offset, &collector_state));
                thread::sleep(task.duration);
                let finished = Instant::now();
                let turnaround = finished.saturating_duration_since(task_origin(task.arrival_offset, &collector_state));

                {
                    let mut collector = collector_state.lock().unwrap_or_else(|poison| poison.into_inner());
                    collector.worker_busy_time[worker_id] += task.duration;
                    collector.records.push(TaskRecord {
                        task_id: task.id,
                        worker_id,
                        kind: task.kind,
                        wait_time: dispatch_delay,
                        turnaround_time: turnaround,
                        cpu_cost: task.cpu_cost,
                    });
                }

                {
                    let mut state = shared_state.lock().unwrap_or_else(|poison| poison.into_inner());
                    state.busy_workers = state.busy_workers.saturating_sub(1);
                    state.current_cpu_load = (state.current_cpu_load - task.cpu_cost).max(0.0);
                }

                let _ = ready_tx.send(worker_id);
            }
            WorkerMessage::Shutdown => break,
        }
    }
}

fn task_origin(arrival_offset: Duration, collector_state: &Arc<Mutex<CollectorState>>) -> Instant {
    let collector = collector_state.lock().unwrap_or_else(|poison| poison.into_inner());
    collector.run_start + arrival_offset
}

fn monitor_loop(shared_state: Arc<Mutex<SharedState>>, run_start: Instant) -> Vec<MonitorSample> {
    let mut samples = Vec::new();

    loop {
        thread::sleep(Duration::from_millis(MONITOR_INTERVAL_MS));

        let snapshot = {
            let state = shared_state.lock().unwrap_or_else(|poison| poison.into_inner());
            (
                state.total_queue_len,
                state.cpu_queue_len,
                state.io_queue_len,
                state.busy_workers,
                state.current_cpu_load,
                state.done,
            )
        };

        samples.push(MonitorSample {
            elapsed: Instant::now().saturating_duration_since(run_start),
            queue_len: snapshot.0,
            cpu_queue_len: snapshot.1,
            io_queue_len: snapshot.2,
            busy_workers: snapshot.3,
            cpu_load: snapshot.4,
        });

        if snapshot.5 && snapshot.0 == 0 && snapshot.3 == 0 {
            break;
        }
    }

    samples
}

fn update_queue_state(
    shared_state: &Arc<Mutex<SharedState>>,
    fifo_queue: &VecDeque<Task>,
    cpu_queue: &VecDeque<Task>,
    io_queue: &VecDeque<Task>,
) {
    let mut state = shared_state.lock().unwrap_or_else(|poison| poison.into_inner());
    state.cpu_queue_len = cpu_queue.len();
    state.io_queue_len = io_queue.len();
    state.total_queue_len = fifo_queue.len() + cpu_queue.len() + io_queue.len();
}

fn summarize_run(
    config: ExperimentConfig,
    collector: &CollectorState,
    monitor_samples: &[MonitorSample],
) -> RunSummary {
    let total_tasks_completed = collector.records.len();
    let makespan = collector.run_start.elapsed();

    let mut total_wait = Duration::ZERO;
    let mut total_turnaround = Duration::ZERO;
    let mut max_wait = Duration::ZERO;
    let mut cpu_tasks_completed = 0usize;
    let mut io_tasks_completed = 0usize;

    let mut cpu_wait_sum_ms = 0.0;
    let mut io_wait_sum_ms = 0.0;

    for record in &collector.records {
        total_wait += record.wait_time;
        total_turnaround += record.turnaround_time;
        if record.wait_time.cmp(&max_wait) == Ordering::Greater {
            max_wait = record.wait_time;
        }

        match record.kind {
            TaskKind::Cpu => {
                cpu_tasks_completed += 1;
                cpu_wait_sum_ms += record.wait_time.as_secs_f64() * 1000.0;
            }
            TaskKind::Io => {
                io_tasks_completed += 1;
                io_wait_sum_ms += record.wait_time.as_secs_f64() * 1000.0;
            }
        }
    }

    let average_wait = divide_duration(total_wait, total_tasks_completed);
    let average_turnaround = divide_duration(total_turnaround, total_tasks_completed);

    let average_cpu_usage = if monitor_samples.is_empty() {
        0.0
    } else {
        monitor_samples.iter().map(|sample| sample.cpu_load).sum::<f64>() / monitor_samples.len() as f64
    };

    let average_busy_workers = if monitor_samples.is_empty() {
        0.0
    } else {
        monitor_samples
            .iter()
            .map(|sample| sample.busy_workers as f64)
            .sum::<f64>()
            / monitor_samples.len() as f64
    };

    let max_queue_len = monitor_samples.iter().map(|sample| sample.queue_len).max().unwrap_or(0);
    let average_queue_len = if monitor_samples.is_empty() {
        0.0
    } else {
        monitor_samples
            .iter()
            .map(|sample| sample.queue_len as f64)
            .sum::<f64>()
            / monitor_samples.len() as f64
    };

    let worker_utilization = collector
        .worker_busy_time
        .iter()
        .map(|busy| {
            if makespan.is_zero() {
                0.0
            } else {
                busy.as_secs_f64() / makespan.as_secs_f64()
            }
        })
        .collect::<Vec<_>>();

    let cpu_wait_avg_ms = if cpu_tasks_completed == 0 {
        0.0
    } else {
        cpu_wait_sum_ms / cpu_tasks_completed as f64
    };
    let io_wait_avg_ms = if io_tasks_completed == 0 {
        0.0
    } else {
        io_wait_sum_ms / io_tasks_completed as f64
    };

    RunSummary {
        config,
        total_tasks_completed,
        makespan,
        average_wait,
        average_turnaround,
        max_wait,
        average_cpu_usage,
        average_busy_workers,
        max_queue_len,
        average_queue_len,
        cpu_tasks_completed,
        io_tasks_completed,
        worker_utilization,
        fairness_gap_ms: (cpu_wait_avg_ms - io_wait_avg_ms).abs(),
    }
}

fn divide_duration(total: Duration, count: usize) -> Duration {
    if count == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(total.as_secs_f64() / count as f64)
    }
}

fn generate_tasks(config: &ExperimentConfig) -> Vec<Task> {
    let mut rng = SmallRng::seed_from_u64(config.seed);
    let mut tasks = Vec::with_capacity(config.task_count);
    let mut next_offset = Duration::ZERO;

    for id in 0..config.task_count {
        let kind = match config.workload {
            WorkloadKind::Balanced => {
                if rng.gen_bool(0.70) {
                    TaskKind::Io
                } else {
                    TaskKind::Cpu
                }
            }
            WorkloadKind::Stressed => {
                if rng.gen_bool(0.35) {
                    TaskKind::Io
                } else {
                    TaskKind::Cpu
                }
            }
        };

        let burst_multiplier = match config.workload {
            WorkloadKind::Balanced => 1,
            WorkloadKind::Stressed => {
                if id % 100 < 35 { 0 } else { 1 }
            }
        };

        let interval_ms = if burst_multiplier == 0 {
            rng.gen_range(0..=2)
        } else {
            let low = config.base_interval_ms.saturating_sub(6);
            let high = config.base_interval_ms + 6;
            rng.gen_range(low..=high)
        };
        next_offset += Duration::from_millis(interval_ms);

        let (duration_ms, cpu_cost) = match (config.workload, kind) {
            (WorkloadKind::Balanced, TaskKind::Io) => (rng.gen_range(180..=220), 0.10),
            (WorkloadKind::Balanced, TaskKind::Cpu) => (rng.gen_range(180..=220), 0.40),
            (WorkloadKind::Stressed, TaskKind::Io) => (rng.gen_range(120..=240), 0.10),
            (WorkloadKind::Stressed, TaskKind::Cpu) => (rng.gen_range(240..=380), 0.40),
        };

        tasks.push(Task {
            id,
            arrival_offset: next_offset,
            kind,
            duration: Duration::from_millis(duration_ms),
            cpu_cost,
        });
    }

    tasks
}

fn print_summary(summary: &RunSummary) {
    println!("Run name: {}", summary.config.name);
    println!("Tasks completed: {}", summary.total_tasks_completed);
    println!("Makespan: {:.3} s", summary.makespan.as_secs_f64());
    println!("Average wait: {:.3} ms", summary.average_wait.as_secs_f64() * 1000.0);
    println!(
        "Average turnaround: {:.3} ms",
        summary.average_turnaround.as_secs_f64() * 1000.0
    );
    println!("Max wait: {:.3} ms", summary.max_wait.as_secs_f64() * 1000.0);
    println!("Average simulated CPU usage: {:.2}%", summary.average_cpu_usage * 100.0);
    println!("Average busy workers: {:.2}", summary.average_busy_workers);
    println!("Max queue length: {}", summary.max_queue_len);
    println!("Average queue length: {:.2}", summary.average_queue_len);
    println!(
        "Completed by kind: CPU = {}, IO = {}",
        summary.cpu_tasks_completed, summary.io_tasks_completed
    );
    println!("Fairness gap (avg wait difference): {:.3} ms", summary.fairness_gap_ms);
    println!("Worker utilization:");
    for (index, utilization) in summary.worker_utilization.iter().enumerate() {
        println!("  Worker {:02}: {:>6.2}%", index, utilization * 100.0);
    }
}

fn print_comparison(results: &[RunSummary]) {
    println!("\n==================== Final Comparison ====================");
    println!(
        "{:<20} {:<11} {:>10} {:>12} {:>12} {:>12}",
        "Run", "Policy", "Makespan", "Avg Wait", "CPU Avg", "Max Queue"
    );

    for result in results {
        println!(
            "{:<20} {:<11} {:>9.3}s {:>10.2}ms {:>10.2}% {:>12}",
            result.config.name,
            result.config.policy.to_string(),
            result.makespan.as_secs_f64(),
            result.average_wait.as_secs_f64() * 1000.0,
            result.average_cpu_usage * 100.0,
            result.max_queue_len
        );
    }

    println!("\nInterpretation:");
    println!("- FIFO is simple, but it can suffer head-of-line blocking when a CPU-heavy task sits at the front and there is not enough remaining CPU budget to dispatch it.");
    println!("- The optimized policy keeps separate CPU and IO queues, reserves two workers for CPU work, and lets non-reserved workers prefer IO first. This usually lowers queue buildup and improves turnaround when the workload becomes CPU-heavy.");
    println!("- Balanced workload should show both policies working reasonably well. Stressed workload is where the optimized design should pull ahead by keeping IO jobs flowing while CPU jobs consume more of the 100% global CPU budget.");
}
