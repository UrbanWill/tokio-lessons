use rand::random_range;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

const MAX_CONCURRENT: usize = 5;
const NUM_TASKS: usize = 20;
const MIN_SLEEP_MS: u64 = 100;
const MAX_SLEEP_MS: u64 = 500;

#[tokio::main]
async fn main() {
    let main_start = Instant::now();

    let (tx, mut rx) = mpsc::channel(NUM_TASKS);

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let mut workers = JoinSet::new();

    let handle = tokio::spawn(async move {
        loop {
            let Some(result) = rx.recv().await else {
                break;
            };
            println!("{}", result);
        }
        Ok::<String, ()>("Collector task completed".to_string())
    });

    for id in 0..NUM_TASKS {
        let tx = tx.clone();
        let semaphore = semaphore.clone();

        workers.spawn(async move {
            let Ok(_permit) = semaphore.acquire().await else {
                eprintln!("Task {} failed to acquire semaphore permit", id);
                return Err(id);
            };

            let task_start = Instant::now();

            tx.send(format!(
                "Task {} started at {:?} (Currently in flight: {})",
                id,
                task_start,
                MAX_CONCURRENT - semaphore.available_permits()
            ))
            .await
            .unwrap_or_else(|e| {
                eprintln!(
                    "Failed to send start message for task {}: {:?}",
                    id, e
                );
            });

            tokio::time::sleep(Duration::from_millis(random_range(
                MIN_SLEEP_MS..=MAX_SLEEP_MS,
            )))
            .await;

            let task_end = task_start.elapsed();
            tx.send(format!("Task {} completed in {:.2?}", id, task_end))
                .await
                .unwrap_or_else(|e| {
                    eprintln!(
                        "Failed to send completion message for task {}: {:?}",
                        id, e
                    );
                });

            let result = format!("Task {} completed", id);
            Ok::<String, usize>(result)
        });
    }

    drop(tx);

    while let Some(res) = workers.join_next().await {
        match res {
            Ok(Ok(_msg)) => {} // success, collector already printed teh details.
            Ok(Err(id)) => eprintln!("Task {} returned error", id),
            Err(e) if e.is_panic() => eprint!("Task panicked {}", e),
            Err(e) => eprintln!("Task cancelled {}", e),
        }
    }

    match handle.await {
        Ok(inner) => match inner {
            Ok(result) => println!("{}", result),
            Err(e) => eprintln!("Collector error: {:?}", e),
        },
        Err(e) => eprintln!("Error {}", e),
    }

    let main_elapsed = main_start.elapsed();

    println!("Total elapsed: {}", main_elapsed.as_millis());
}
