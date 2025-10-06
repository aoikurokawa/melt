use std::{collections::HashMap, fs::File, io::Write, str::FromStr, time::Instant};

use anyhow::Result;
use clap::Parser;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use solana_transaction_status::{EncodedTransaction, UiParsedInstruction, UiTransactionEncoding};

#[derive(Parser, Debug)]
#[command(name = "cranker-expense")]
#[command(about = "Analyze Solana cranker account expenses by program", long_about = None)]
struct Args {
    /// The cranker account addresses to analyze (comma-separated)
    #[arg(short, long, value_delimiter = ',')]
    address: Vec<String>,

    /// RPC endpoint URL
    #[arg(short, long)]
    rpc_url: String,

    /// Output CSV file path
    #[arg(short, long, default_value = "cranker_expenses.csv")]
    output: String,

    /// Concurrent requests
    #[arg(short = 'c', long, default_value = "50")]
    concurrency: usize,

    /// Start epoch (inclusive)
    #[arg(long)]
    start_epoch: u64,

    /// End epoch (inclusive)
    #[arg(long)]
    end_epoch: u64,
}

#[derive(Debug, Clone)]
struct ProgramExpense {
    account: String,
    epoch: u64,
    program_id: String,
    transaction_count: usize,
    total_fees_lamports: u64,
}

const SLOTS_PER_EPOCH: u64 = 432000;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let start_time = Instant::now();

    println!("🔍 Analyzing {} cranker account(s)", args.address.len());
    for addr in &args.address {
        println!("  - {}", addr);
    }

    let min_slot = args.start_epoch * SLOTS_PER_EPOCH;
    let max_slot = (args.end_epoch + 1) * SLOTS_PER_EPOCH - 1;

    println!(
        "📅 Analyzing epochs {} to {}",
        args.start_epoch, args.end_epoch
    );
    println!("📍 Slot range: {} to {}", min_slot, max_slot);
    println!("📡 Using RPC: {}", args.rpc_url);
    println!("⚡ Concurrency: {}\n", args.concurrency);

    let client =
        RpcClient::new_with_commitment(args.rpc_url.clone(), CommitmentConfig::confirmed());

    let mut all_program_expenses: HashMap<(String, u64, String), ProgramExpense> = HashMap::new();
    let mut grand_total_fees = 0u64;
    let mut grand_total_processed = 0usize;

    // Process each address
    for (addr_idx, address) in args.address.iter().enumerate() {
        println!("{:-<80}", "");
        println!(
            "📊 Processing address {}/{}: {}",
            addr_idx + 1,
            args.address.len(),
            address
        );
        println!("{:-<80}", "");

        let pubkey = Pubkey::from_str(address)?;

        let mut before = None;
        let mut should_break = false;
        let mut batch_count = 0;
        let mut total_fetched = 0;
        let mut program_expenses: HashMap<(u64, String), ProgramExpense> = HashMap::new();
        let mut total_fees = 0u64;
        let mut processed = 0;

        loop {
            // Fetch batch of signatures
            let signatures = client
                .get_signatures_for_address_with_config(
                    &pubkey,
                    solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config {
                        before,
                        until: None,
                        limit: Some(1000),
                        commitment: Some(CommitmentConfig::confirmed()),
                    },
                )
                .await?;

            if signatures.is_empty() {
                break;
            }

            batch_count += 1;
            total_fetched += signatures.len();

            // Set before for next iteration
            before = signatures
                .last()
                .map(|s| Signature::from_str(&s.signature).unwrap());

            // Filter signatures by slot range
            let valid_signatures: Vec<_> = signatures
                .into_iter()
                .filter_map(|sig| {
                    if sig.slot < min_slot {
                        should_break = true;
                        None
                    } else if sig.slot <= max_slot {
                        Some(sig)
                    } else {
                        None
                    }
                })
                .collect();

            if valid_signatures.is_empty() {
                if should_break {
                    break;
                }
                continue;
            }

            println!(
                "  Batch {}: {} valid signatures (total fetched: {})",
                batch_count,
                valid_signatures.len(),
                total_fetched
            );

            // Fetch and process transactions in chunks
            let chunk_size = args.concurrency;
            let chunks: Vec<_> = valid_signatures
                .chunks(chunk_size)
                .map(|c| c.to_vec())
                .collect();

            for chunk in chunks {
                let mut tasks = vec![];

                for sig_info in &chunk {
                    let client = RpcClient::new_with_commitment(
                        args.rpc_url.clone(),
                        CommitmentConfig::confirmed(),
                    );
                    let signature_str = sig_info.signature.clone();
                    let slot = sig_info.slot;

                    let task = tokio::spawn(async move {
                        if let Ok(signature) = Signature::from_str(&signature_str) {
                            match client
                                .get_transaction(&signature, UiTransactionEncoding::JsonParsed)
                                .await
                            {
                                Ok(tx) => {
                                    let mut program_ids = Vec::new();
                                    let mut fee = 0;

                                    if let Some(meta) = &tx.transaction.meta {
                                        fee = meta.fee;
                                    }

                                    if let EncodedTransaction::Json(ui_tx) =
                                        tx.transaction.transaction
                                    {
                                        match ui_tx.message {
                                            solana_transaction_status::UiMessage::Parsed(
                                                parsed_msg,
                                            ) => {
                                                for instruction in &parsed_msg.instructions {
                                                    match instruction {
                                                        solana_transaction_status::UiInstruction::Parsed(parsed_ix) => {
                                                            match parsed_ix {
                                                                UiParsedInstruction::Parsed(ui_parsed_ix) => {
                                                                    program_ids.push(ui_parsed_ix.program_id.clone());
                                                                }
                                                                UiParsedInstruction::PartiallyDecoded(ui_partial_decoded_ix) => {
                                                                    program_ids.push(ui_partial_decoded_ix.program_id.clone());
                                                                }
                                                            }
                                                        }
                                                        solana_transaction_status::UiInstruction::Compiled(compiled_ix) => {
                                                            let idx = compiled_ix.program_id_index as usize;
                                                            if idx < parsed_msg.account_keys.len() {
                                                                program_ids.push(parsed_msg.account_keys[idx].pubkey.clone());
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            solana_transaction_status::UiMessage::Raw(raw_msg) => {
                                                for instruction in &raw_msg.instructions {
                                                    let idx = instruction.program_id_index as usize;
                                                    if idx < raw_msg.account_keys.len() {
                                                        program_ids.push(
                                                            raw_msg.account_keys[idx].clone(),
                                                        );
                                                    }
                                                }
                                            }
                                        }

                                        if !program_ids.is_empty() {
                                            return Some((slot, fee, program_ids));
                                        }
                                    }
                                }
                                Err(_) => {}
                            }
                        }
                        None
                    });

                    tasks.push(task);
                }

                let results = futures::future::join_all(tasks).await;

                for result in results {
                    if let Ok(Some((slot, fee, program_ids))) = result {
                        total_fees += fee;
                        processed += 1;

                        let epoch = slot / SLOTS_PER_EPOCH;

                        for program_id in program_ids {
                            program_expenses
                                .entry((epoch, program_id.clone()))
                                .and_modify(|e| {
                                    e.transaction_count += 1;
                                    e.total_fees_lamports += fee;
                                })
                                .or_insert(ProgramExpense {
                                    account: address.clone(),
                                    epoch,
                                    program_id,
                                    transaction_count: 1,
                                    total_fees_lamports: fee,
                                });
                        }
                    }
                }
            }

            println!("    Processed {} transactions so far", processed);

            if should_break {
                break;
            }
        }

        // Add to global expenses
        for (_, expense) in program_expenses {
            all_program_expenses
                .entry((
                    expense.account.clone(),
                    expense.epoch,
                    expense.program_id.clone(),
                ))
                .and_modify(|e| {
                    e.transaction_count += expense.transaction_count;
                    e.total_fees_lamports += expense.total_fees_lamports;
                })
                .or_insert(expense);
        }

        grand_total_fees += total_fees;
        grand_total_processed += processed;

        println!(
            "✓ Address complete: {} transactions, {} SOL in fees\n",
            processed,
            total_fees as f64 / 1e9
        );
    }

    let duration = start_time.elapsed();
    println!("{:-<80}", "");
    println!(
        "✓ All addresses completed in {:.2}s",
        duration.as_secs_f64()
    );
    println!(
        "✓ Total: {} transactions, {:.9} SOL in fees\n",
        grand_total_processed,
        grand_total_fees as f64 / 1e9
    );

    // Sort by epoch desc, then total fees descending
    let mut expenses: Vec<_> = all_program_expenses.into_iter().map(|(_, v)| v).collect();
    expenses.sort_by(|a, b| {
        b.epoch
            .cmp(&a.epoch)
            .then(b.total_fees_lamports.cmp(&a.total_fees_lamports))
    });

    // Display results
    println!("{}", "=".repeat(95));
    println!("📈 EXPENSE BREAKDOWN BY EPOCH AND PROGRAM");
    println!("{}\n", "=".repeat(95));

    println!(
        "{:<8} {:<45} {:>12} {:>15}",
        "Epoch", "Program ID", "Tx Count", "Total Fees (SOL)"
    );
    println!("{:-<95}", "");

    for expense in &expenses {
        let sol_amount = expense.total_fees_lamports as f64 / 1e9;
        println!(
            "{:<8} {:<45} {:>12} {:>15.9}",
            expense.epoch,
            &expense.program_id[..std::cmp::min(44, expense.program_id.len())],
            expense.transaction_count,
            sol_amount
        );
    }

    println!("{:-<95}", "");
    println!(
        "{:<8} {:<45} {:>12} {:>15.9}",
        "TOTAL",
        "",
        grand_total_processed,
        grand_total_fees as f64 / 1e9
    );
    println!("{}\n", "=".repeat(95));

    // Export to CSV
    println!("💾 Exporting to CSV: {}", args.output);
    export_to_csv(&expenses, &args.output)?;
    println!("✅ Export complete!\n");

    Ok(())
}

fn export_to_csv(expenses: &[ProgramExpense], filepath: &str) -> Result<()> {
    let mut file = File::create(filepath)?;

    writeln!(
        file,
        "account,epoch,program_id,transaction_count,total_fees_lamports,total_fees_sol"
    )?;

    for expense in expenses {
        writeln!(
            file,
            "{},{},{},{},{},{:.9}",
            expense.account,
            expense.epoch,
            expense.program_id,
            expense.transaction_count,
            expense.total_fees_lamports,
            expense.total_fees_lamports as f64 / 1e9
        )?;
    }

    Ok(())
}
