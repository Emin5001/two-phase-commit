// Crates
extern crate ctrlc;
extern crate stderrlog;

// Rust libs
use clap::Parser;
use ipc_channel::ipc::{channel, IpcOneShotServer, IpcReceiver, IpcSender};
use std::env;
use std::fs;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// Local modules
pub mod checker;
pub mod client;
pub mod common;
pub mod coordinator;
pub mod message;
pub mod oplog;
pub mod participant;
pub mod tpcopts;

use client::Client;
use coordinator::Coordinator;
use message::ProtocolMessage;
use oplog::OpLog;
use participant::Participant;

// Child sends its own IpcSender and IpcReceiver endpoints to the Parent, and vice versa
type ParentChannels = (IpcSender<ProtocolMessage>, IpcReceiver<ProtocolMessage>);

///
/// Spawns a child process.
///
/// Returns the IpcSender of Parent->Child, and IpcReceiver of Child->Parent.
///
fn spawn_child(
    opts: &tpcopts::TPCOptions,
) -> (
    Child,
    IpcSender<ProtocolMessage>,
    IpcReceiver<ProtocolMessage>,
) {
    let (server, server_name) =
        IpcOneShotServer::<ParentChannels>::new().expect("Failed to create IPC bootstrap server.");

    let mut child_opts = opts.clone();
    child_opts.ipc_path = server_name;

    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process.");

    // Block until we get the channel endpoints from the child
    let (_, (tx_to_child, rx_from_child)) = server
        .accept()
        .expect("Failed to accept child's IPC handshake.");

    (child, tx_to_child, rx_from_child)
}

///
/// Child that gets spawned has to connect back to the Parent that spawns it. This fn helps with that.
///
/// Returns the endpoints the child kept, within the child process.
///
fn connect_to_coordinator(
    opts: &tpcopts::TPCOptions,
) -> (IpcSender<ProtocolMessage>, IpcReceiver<ProtocolMessage>) {
    let bootstrap_tx = IpcSender::<ParentChannels>::connect(opts.ipc_path.clone())
        .expect("Failed to connect to coordinator's IPC bootstrap server");

    let (tx_to_child, rx_from_parent) =
        channel::<ProtocolMessage>().expect("Failed to create parent-to-child channel.");
    let (tx_to_parent, rx_from_child) =
        channel::<ProtocolMessage>().expect("Failed to create child-to-parent channel.");

    // Send the endpoints of the child to the parent - it's blocking, waiting for us!
    bootstrap_tx
        .send((tx_to_child, rx_from_child))
        .expect("Failed to send IPC endpoints to coordinator");

    (tx_to_parent, rx_from_parent)
}

//
fn run(opts: &tpcopts::TPCOptions, running: Arc<AtomicBool>) {
    let log = OpLog::new(format!("{}/coordinator.log", opts.log_path));
    let mut coordinator = Coordinator::new(log, running);
    let mut children = Vec::new();

    // Start participants before clients begin sending requests
    for num in 0..opts.num_participants {
        let mut child_opts = opts.clone();
        child_opts.mode = "participant".to_string();
        child_opts.num = num;
        let (child, tx, rx) = spawn_child(&child_opts);
        children.push(child);
        coordinator.add_participant(format!("participant_{}", num), tx, rx);
    }
    for num in 0..opts.num_clients {
        let mut child_opts = opts.clone();
        child_opts.mode = "client".to_string();
        child_opts.num = num;
        let (child, tx, rx) = spawn_child(&child_opts);
        children.push(child);
        coordinator.add_client(format!("client_{}", num), tx, rx);
    }

    coordinator.protocol();
    let mut failed = false;
    for mut child in children {
        match child.wait() {
            Ok(status) if status.success() => {}
            result => {
                eprintln!("Child process failed: {:?}", result);
                failed = true;
            }
        }
    }
    assert!(!failed, "A child process failed");
}

//
fn run_client(opts: &tpcopts::TPCOptions, running: Arc<AtomicBool>) {
    let client_id_str = format!("client_{}", opts.num);
    let client_log_path = format!("{}//{}.log", opts.log_path, client_id_str);

    let log = OpLog::new(client_log_path);

    let (tx_to_coordinator, rx_from_coordinator) = connect_to_coordinator(opts);

    let mut client = Client::new(
        client_id_str,
        opts.num,
        log,
        opts.num_requests,
        running,
        tx_to_coordinator,
        rx_from_coordinator,
    );

    // Kick off the Client for the 2PC Protocol.
    client.protocol();
}

//
fn run_participant(opts: &tpcopts::TPCOptions, running: Arc<AtomicBool>) {
    let participant_id_str = format!("participant_{}", opts.num);
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);

    let log = OpLog::new(participant_log_path);

    let (tx_to_coordinator, rx_from_coordinator) = connect_to_coordinator(opts);

    let mut participant = Participant::new(
        participant_id_str,
        log,
        opts.operation_success_probability,
        opts.send_success_probability,
        running,
        tx_to_coordinator,
        rx_from_coordinator,
    );

    // Kick off the Participant for the 2PC Protocol.
    participant.protocol();
}

//
fn run_check(opts: &tpcopts::TPCOptions) {
    checker::check_last_run(
        opts.num_clients,
        opts.num_requests,
        opts.num_participants,
        &opts.log_path,
    );
}

fn main() {
    let opts = tpcopts::TPCOptions::parse();
    if let Err(err) = opts.validate() {
        eprintln!("{}", err);
        std::process::exit(2);
    }

    stderrlog::new()
        .module(module_path!())
        .quiet(false)
        .verbosity(opts.verbosity)
        .timestamp(stderrlog::Timestamp::Millisecond)
        .init()
        .unwrap();

    if opts.mode != "check" {
        fs::create_dir_all(&opts.log_path).expect("Failed to create log path");
    }

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler.");

    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => run_check(&opts),
        _ => panic!("Error: Mode is not run, client, participant, or check."),
    }
}
