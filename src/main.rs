use std::collections::HashMap;
use std::time::{Duration, Instant};
use anyhow::Result;
use futures::{StreamExt, SinkExt};
use serde::Deserialize;
use tonic::transport::channel::ClientTlsConfig;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
};
use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;

// ── config ──────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct Config {
    duration_secs: u64,
    endpoints: Vec<Endpoint>,
}

#[derive(Deserialize, Clone)]
struct Endpoint {
    name: String,
    url: String,
    x_token: Option<String>,
}

// ── programs to subscribe to ────────────────────────────────────────────────

const ACCOUNT_OWNERS: &[&str] = &[
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", // Meteora
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", // PumpFun
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", // CLMM
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", // Raydium AMM
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", // Orca
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", // Raydium CPMM
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", // Meteora Dyn
    "24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi", // Meteora Vault
    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG", // Meteora Dyn V2
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", // SPL Token
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", // Token-2022
];

// Token programs banned from transactions_status on some providers
const TXSTATUS_PROGRAMS: &[&str] = &[
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",
    "24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi",
    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",
];

// ── per-endpoint results ────────────────────────────────────────────────────

struct Results {
    name: String,
    url: String,
    elapsed: f64,
    total_accounts: u64,
    total_txstatus: u64,
    matched_sigs: usize,
    acct_first: u64,
    txst_first: u64,
    error: Option<String>,
}

// ── test one endpoint ───────────────────────────────────────────────────────

async fn test_endpoint(ep: &Endpoint, duration_secs: u64) -> Results {
    let mut r = Results {
        name: ep.name.clone(),
        url: ep.url.clone(),
        elapsed: 0.0,
        total_accounts: 0,
        total_txstatus: 0,
        matched_sigs: 0,
        acct_first: 0,
        txst_first: 0,
        error: None,
    };

    let res = run_test(ep, duration_secs, &mut r).await;
    if let Err(e) = res {
        r.error = Some(format!("{}", e));
    }
    r
}

async fn run_test(ep: &Endpoint, duration_secs: u64, r: &mut Results) -> Result<()> {
    eprintln!("[{}] Connecting to {}...", ep.name, ep.url);

    let builder = GeyserGrpcClient::build_from_shared(ep.url.clone())?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .max_decoding_message_size(1024 * 1024 * 1024);

    let builder = if let Some(ref token) = ep.x_token {
        builder.x_token(Some(token.clone()))?
    } else {
        builder
    };

    let mut client = builder.connect().await?;
    eprintln!("[{}] Connected. Subscribing...", ep.name);

    let mut accounts = HashMap::new();
    accounts.insert("accts".to_string(), SubscribeRequestFilterAccounts {
        account: vec![],
        owner: ACCOUNT_OWNERS.iter().map(|s| s.to_string()).collect(),
        filters: vec![],
        nonempty_txn_signature: None,
    });

    let mut transactions_status = HashMap::new();
    transactions_status.insert("txst".to_string(), SubscribeRequestFilterTransactions {
        vote: Some(false),
        failed: Some(false),
        account_include: TXSTATUS_PROGRAMS.iter().map(|s| s.to_string()).collect(),
        account_exclude: vec![],
        account_required: vec![],
        signature: None,
    });

    let (mut tx, mut stream) = client.subscribe_with_request(Some(SubscribeRequest {
        accounts,
        transactions_status,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    })).await?;

    eprintln!("[{}] Subscribed. Collecting for {}s...", ep.name, duration_secs);

    let start = Instant::now();
    let deadline = Duration::from_secs(duration_secs);

    let mut sig_has_account: HashMap<[u8; 64], bool> = HashMap::new();
    let mut sig_has_txstatus: HashMap<[u8; 64], bool> = HashMap::new();
    let mut last_print = Instant::now();

    while start.elapsed() < deadline {
        let msg = tokio::select! {
            m = stream.next() => m,
            _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
        };
        let msg = match msg {
            Some(Ok(m)) => m,
            Some(Err(e)) => return Err(anyhow::anyhow!("stream error: {}", e)),
            None => return Err(anyhow::anyhow!("stream closed unexpectedly")),
        };

        match msg.update_oneof {
            Some(UpdateOneof::Account(ref account)) => {
                if let Some(ref ad) = account.account {
                    if let Some(ref sig_bytes) = ad.txn_signature {
                        if sig_bytes.len() == 64 {
                            let key: [u8; 64] = sig_bytes[..64].try_into().unwrap();
                            r.total_accounts += 1;
                            sig_has_account.insert(key, true);
                            if sig_has_txstatus.contains_key(&key) {
                                r.txst_first += 1;
                            }
                        }
                    }
                }
            }
            Some(UpdateOneof::TransactionStatus(ref ts)) => {
                if ts.signature.len() == 64 {
                    let key: [u8; 64] = ts.signature[..64].try_into().unwrap();
                    r.total_txstatus += 1;
                    sig_has_txstatus.insert(key, true);
                    if sig_has_account.contains_key(&key) {
                        r.acct_first += 1;
                    }
                }
            }
            Some(UpdateOneof::Ping(_)) => {
                let _ = tx.send(SubscribeRequest {
                    ping: Some(SubscribeRequestPing { id: 1 }),
                    ..Default::default()
                }).await;
            }
            _ => {}
        }

        if last_print.elapsed() > Duration::from_secs(5) {
            eprintln!("[{}] {}s | accts: {} | txst: {} | acct_first: {} | txst_first: {}",
                ep.name, start.elapsed().as_secs(),
                r.total_accounts, r.total_txstatus, r.acct_first, r.txst_first);
            last_print = Instant::now();
        }
    }

    r.elapsed = start.elapsed().as_secs_f64();
    r.matched_sigs = sig_has_account.keys()
        .filter(|k| sig_has_txstatus.contains_key(*k))
        .count();

    Ok(())
}

// ── output ──────────────────────────────────────────────────────────────────

fn print_results(results: &[Results]) {
    println!();
    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║              gRPC Message Ordering Test Results                     ║");
    println!("╚══════════════════════════════════════════════════════════════════════╝");

    for r in results {
        println!();
        println!("┌──────────────────────────────────────────────────────────────────────┐");
        println!("│  {} ({})", r.name, r.url);
        println!("├──────────────────────────────────────────────────────────────────────┤");

        if let Some(ref err) = r.error {
            println!("│  ERROR: {}", err);
            println!("└──────────────────────────────────────────────────────────────────────┘");
            continue;
        }

        println!("│  Duration:                     {:.1}s", r.elapsed);
        println!("│  Account updates received:     {}", r.total_accounts);
        println!("│  TransactionStatus received:   {}", r.total_txstatus);
        println!("│  Signatures seen in both:      {}", r.matched_sigs);
        println!("│");
        println!("│  Account arrived FIRST:        {} (correct)", r.acct_first);
        println!("│  TransactionStatus FIRST:      {} (wrong ordering)", r.txst_first);
        println!("│");

        if r.total_accounts == 0 || r.total_txstatus == 0 {
            println!("│  RESULT: INSUFFICIENT DATA");
        } else if r.txst_first == 0 && r.acct_first > 0 {
            println!("│  RESULT: ✅ CORRECT — accounts always arrive before TransactionStatus");
        } else if r.acct_first == 0 && r.txst_first > 0 {
            println!("│  RESULT: ❌ INVERTED — TransactionStatus always arrives before accounts");
        } else {
            let total = r.acct_first + r.txst_first;
            let pct_bad = r.txst_first as f64 / total as f64 * 100.0;
            if pct_bad > 50.0 {
                println!("│  RESULT: ❌ MOSTLY INVERTED ({:.1}% of updates arrive after their tx_status)", pct_bad);
            } else {
                println!("│  RESULT: ⚠️  MIXED ({:.1}% inverted)", pct_bad);
            }
        }
        println!("└──────────────────────────────────────────────────────────────────────┘");
    }

    if results.len() > 1 {
        println!();
        println!("┌──────────────────────────────────────────────────────────────────────┐");
        println!("│  SUMMARY                                                            │");
        println!("├──────────────────────────────────────────────────────────────────────┤");

        let name_width = results.iter().map(|r| r.name.len()).max().unwrap_or(8).max(8);
        println!("│  {:<width$}  {:>10}  {:>10}  {:>10}  RESULT",
                 "Endpoint", "Acct 1st", "TxSt 1st", "Matched", width = name_width);
        println!("│  {}", "-".repeat(name_width + 50));

        for r in results {
            if r.error.is_some() {
                println!("│  {:<width$}  ERROR", r.name, width = name_width);
                continue;
            }
            let verdict = if r.txst_first == 0 && r.acct_first > 0 {
                "✅ CORRECT"
            } else if r.acct_first == 0 && r.txst_first > 0 {
                "❌ INVERTED"
            } else if r.txst_first > r.acct_first {
                "❌ MOSTLY INVERTED"
            } else {
                "⚠️  MIXED"
            };
            println!("│  {:<width$}  {:>10}  {:>10}  {:>10}  {}",
                     r.name, r.acct_first, r.txst_first, r.matched_sigs, verdict,
                     width = name_width);
        }
        println!("└──────────────────────────────────────────────────────────────────────┘");
    }

    println!();
    println!("  CORRECT:  Account updates arrive first, then TransactionStatus.");
    println!("  INVERTED: TransactionStatus arrives before accounts for the same transaction.");
    println!();
}

// ── main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = std::env::args().nth(1).unwrap_or_else(|| "config.json".to_string());

    let config_str = std::fs::read_to_string(&config_path)
        .map_err(|e| anyhow::anyhow!("Failed to read {}: {}", config_path, e))?;

    let config: Config = serde_json::from_str(&config_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse {}: {}", config_path, e))?;

    if config.endpoints.is_empty() {
        anyhow::bail!("No endpoints in config file");
    }

    eprintln!("Testing {} endpoint(s) for {} seconds each (running in parallel)\n",
              config.endpoints.len(), config.duration_secs);

    let duration = config.duration_secs;
    let mut handles = Vec::new();

    for ep in config.endpoints {
        handles.push(tokio::spawn(async move {
            test_endpoint(&ep, duration).await
        }));
    }

    let mut results = Vec::new();
    for h in handles {
        results.push(h.await?);
    }

    print_results(&results);
    Ok(())
}
