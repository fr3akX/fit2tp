use std::collections::HashSet;
use std::fmt::Write;
use std::io;
use std::path::PathBuf;
use std::time::Duration;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use clap::{CommandFactory, Parser};
use clap_complete::{generate, Generator, Shell};
use fitparser::profile::MesgNum;
use futures_util::{stream, StreamExt};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use lazy_static::lazy_static;
use reqwest::Client;
use serde_json::json;

lazy_static! {
    static ref WORKOUT_TYPES: HashSet<MesgNum> = HashSet::from_iter(vec![
        MesgNum::Workout,
        MesgNum::Session,
        MesgNum::Activity,
        MesgNum::WorkoutSession,
    ]);
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Opts {
    #[arg(long = "generate", value_enum)]
    generator: Option<Shell>,
    #[arg(short, long)]
    fit_file_dir_path: PathBuf,
    #[arg(short, long)]
    auth_bearer_token: String,
    #[arg(long)]
    athlete_id: u64,
    #[arg(short, long, default_value_t = 8)]
    parallelism: u8,
}


#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt: Opts = Opts::parse();
    let mut command = Opts::command();

    if let Some(generator) = opt.generator {
        eprintln!("Generating completion file for {:?}...", generator);
        print_completions(generator, &mut command);
        return Ok(());
    }

    let client = reqwest::ClientBuilder::default().build()?;
    let dir_list = opt.fit_file_dir_path.read_dir()?.collect::<Result<Vec<_>, io::Error>>()?;
    let progress = make_progress(dir_list.len() as u64);

    stream::iter(dir_list.into_iter()).map(|fit_file| {
        let client = client.clone();
        let opt = opt.clone();
        async move {
            if fit_file.path().extension().and_then(|s| s.to_str()) == Some("fit") {
                // eprintln!("Processing FIT file: {}", fit_file.path().display());
                let base64_content = file_as_base64(&fit_file.path())?;
                let filename = fit_file.file_name().into_string().unwrap_or_else(|_| "unknown.fit".to_string());
                if is_workout(&fit_file.path()).await? {
                    do_tr_request(&client, base64_content, &opt.auth_bearer_token, filename, opt.athlete_id).await
                } else {
                    Ok(())
                }
            } else {
                Ok(())
            }
        }
    }).buffer_unordered(opt.parallelism as usize).for_each(|_| async {
        progress.inc(1);
        ()
    }).await;

    Ok(())
}

fn file_as_base64(path: &PathBuf) -> anyhow::Result<String> {
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    Ok(STANDARD.encode(&buffer))
}

async fn do_tr_request(client: &Client, body: String, auth_bearer_token: &str, filename: String, athlete_id: u64) -> Result<(), Box<dyn std::error::Error>> {
    let body = json!({
        "workoutDay": null,
        "data": body,
        "fileName": filename,
        "uploadClient": "TP Web App",
    });
    eprintln!("Uploading FIT file: {}", filename);
    let c = client.post(format!("https://tpapi.trainingpeaks.com/fitness/v6/athletes/{athlete_id}/workouts/filedata"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", auth_bearer_token))
        .json(&body)
        .send()
        .await?;
    if c.status().is_success() {
        eprintln!("Successfully uploaded FIT file: {}", filename);
    } else {
        eprintln!("Failed to upload FIT file: {}, {}", filename, c.text().await?);
    }

    Ok(())
}

fn print_completions<G: Generator>(g: G, cmd: &mut clap::builder::Command) {
    generate(g, cmd, cmd.get_name().to_string(), &mut io::stdout());
}

fn make_progress(sample_count: u64) -> ProgressBar {
    let pb = ProgressBar::new(sample_count);
    pb.enable_steady_tick(Duration::from_secs(1));
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {percent}% {per_sec} {pos}/{len} ({eta})")
        .unwrap()
        .with_key(
            "eta",
            |state: &ProgressState, w: &mut dyn Write| {
                let d = indicatif::HumanDuration(state.eta());
                write!(w, "{d}").unwrap()
            }
        )
        .progress_chars("#>-"));
    pb
}

async fn is_workout(path: &PathBuf) -> anyhow::Result<bool> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let path = path.clone();
    rayon::spawn(move|| {
            let mut fp = std::fs::File::open(path).unwrap();
            let fit = fitparser::from_reader(&mut fp).unwrap();
            let is_workout = fit.iter().any(|r| WORKOUT_TYPES.contains(&r.kind()));
        tx.send(is_workout).unwrap();
    });
    let res = rx.await?;
    Ok(res)
}