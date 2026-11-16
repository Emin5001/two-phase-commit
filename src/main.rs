// Used for dev purposes. TODO: Should remove in final version
#![allow(dead_code)]

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
pub mod message;
pub mod tpcopts;
pub mod participant;
pub mod client;
pub mod coordinator;
pub mod oplog;

use message::ProtocolMessage;
use participant::Participant;
use client::Client;

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
fn run(opts: &tpcopts::TPCOptions, running: Arc<AtomicBool>) {}

//
fn run_client(opts: &tpcopts::TPCOptions, running: Arc<AtomicBool>) {
    let client_id_str = format!("client_{}", opts.num);
    let client_log_path = format!("{}//{}.log", opts.log_path, client_id_str);

    let log = OpLog::new(client_log_path);

    let (tx_to_coordinator, rx_from_coordinator) = connect_to_coordinator(opts);

    let mut client = Client::new(
        log,
        opts.num_requests,
        running,
        tx_to_coordinator,
        rx_from_coordinator
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
        rx_from_coordinator
    );

    // Kick off the Participant for the 2PC Protocol.
    participant.protocol();
}

//
fn run_check(opts: &tpcopts::TPCOptions, running: Arc<AtomicBool>) {}

fn main() {
    let opts = tpcopts::TPCOptions::parse();

    stderrlog::new()
        .module(module_path!())
        .quiet(false)
        .verbosity(opts.verbosity)
        .timestamp(stderrlog::Timestamp::Millisecond)
        .init()
        .unwrap();

    match fs::create_dir_all(&opts.log_path) {
        Err(e) => error!(
            "Failed to create log path: \"{:?}\". Error \"{:?}\"",
            opts.log_path, e
        ),
        _ => (),
    }

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler.");

    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(_ & opts, running),
        "participant" => run_participant(&opts, running),
        "check" => run_check(&opts, running),
        _ => panic!("Error: Mode is not run, client, participant, or check."),
    }
}
