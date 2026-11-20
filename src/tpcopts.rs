//!
//! TPCOptions
//! A simple tool for managing command line options and trace/log/debug
//! instrumentation for the _T_wo _P_hase _C_ommit project. Exports a
//! constructor for a struct that represents command line options for the
//! project, uses the clap crate to collect command line options, and use the
//! log and stderrlog crates to initialize the application to use trace!(),
//! debug!(), info!() etc macros from the log crate.
//!
use clap::Parser;

#[derive(Clone, Debug, Parser)]
#[command(
    name = "two_phase_commit",
    version,
    about = "2PC exercise written in Rust"
)]
pub struct TPCOptions {
    #[arg(short = 'S', default_value_t = 1.0, value_parser = probability)]
    pub send_success_probability: f64, // Probability that a message send succeeds
    #[arg(short = 's', default_value_t = 1.0, value_parser = probability)]
    pub operation_success_probability: f64, // Probability that an operation succeeds
    #[arg(short = 'c', default_value_t = 3)]
    pub num_clients: u32, // Number of concurrent clients issuing requests
    #[arg(short = 'r', default_value_t = 15)]
    pub num_requests: u32, // Number of requests issued per client
    #[arg(short = 'p', default_value_t = 3)]
    pub num_participants: u32, // Number of participants in 2PC protocol (not including coordinator)
    #[arg(short = 'v', default_value_t = 0)]
    pub verbosity: usize, // Integer verbosity level. experiment with 0 (default) to 5 (fire-hose of output)
    #[arg(short = 'm', default_value = "run", value_parser = ["run", "client", "participant", "check"])]
    pub mode: String, // One of "run", "client", "participant", "check"
    #[arg(short = 'l', default_value = "./logs/")]
    pub log_path: String, // Directory for client, participant, and coordinator logs
    #[arg(long = "ipc_path", default_value = "none")]
    pub ipc_path: String, // Path to IPC socket for setting up communication with the coordinator
    #[arg(long, default_value_t = 0)]
    pub num: u32, // Participant / Client number for naming the log files
}

fn probability(value: &str) -> Result<f64, String> {
    let probability = value.parse::<f64>().map_err(|err| err.to_string())?;
    if probability.is_finite() && (0.0..=1.0).contains(&probability) {
        Ok(probability)
    } else {
        Err("Probability must be between 0 and 1".to_string())
    }
}

impl TPCOptions {
    ///
    /// Check options before starting the protocol
    ///
    pub fn validate(&self) -> Result<(), String> {
        if matches!(self.mode.as_str(), "client" | "participant") && self.ipc_path == "none" {
            return Err("No ipc_path specified for child process".to_string());
        }
        if self.num_clients.checked_mul(self.num_requests).is_none() {
            return Err("Number of transactions exceeds the transaction ID range".to_string());
        }
        Ok(())
    }

    ///
    /// Convert this struct to a vector of CLI options
    ///
    /// HINT: This can be useful for passing arguments to children processes
    ///       using the std::process::Command's args(..) function
    pub fn as_vec(&self) -> Vec<String> {
        vec![
            format!("-S{}", self.send_success_probability),
            format!("-s{}", self.operation_success_probability),
            format!("-c{}", self.num_clients),
            format!("-r{}", self.num_requests),
            format!("-p{}", self.num_participants),
            format!("-v{}", self.verbosity),
            format!("-m{}", self.mode),
            format!("-l{}", self.log_path),
            format!("--ipc_path={}", self.ipc_path),
            format!("--num={}", self.num),
        ]
    }
}
