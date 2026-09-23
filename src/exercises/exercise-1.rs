use anyhow::Result;
use reqwest::StatusCode;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[derive(Debug)]
struct MyResponse {
    url: String,
    status: StatusCode,
    elapsed: Duration,
}

async fn fetch_url(url: String) -> Result<MyResponse> {
    let start = Instant::now();

    let req = reqwest::get(&url).await?;

    let res = MyResponse {
        url,
        status: req.status(),
        elapsed: start.elapsed(),
    };

    Ok(res)
}

#[tokio::main]
async fn main() -> Result<()> {
    let start = Instant::now();
    let urls = vec![
        "https://httpbin.org",
        "https://jsonplaceholder.typicode.com",
        "https://api.ipify.org/",
        "https://quotes.toscrape.com/",
        "https://api.weather.gov/",
        "https://a.bad.url",
    ];

    let (tx, mut rx) = mpsc::channel(urls.len());

    for url in urls {
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            let req = fetch_url(url.to_string()).await;
            let _ = tx_clone
                .send(req.map_err(|e| anyhow::anyhow!("{url}: {e}")))
                .await;
        });
    }

    drop(tx);

    while let Some(result) = rx.recv().await {
        match result {
            Ok(res) => {
                println!(
                    "url: {}, status: {}, elapsed {}ms",
                    res.url,
                    res.status,
                    res.elapsed.as_millis()
                )
            }
            Err(e) => eprintln!("Error: {e}"),
        }
    }

    let total_elapsed = start.elapsed();

    println!("Total elapsed: {}ms", total_elapsed.as_millis());

    Ok(())
}
