///
/// participant.rs
///
/// Implements participant of the 2PC Protocol
///

// Crates
extern crate rand;

// Rust libs
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool};
use ipc_channel::ipc::{IpcReceiver, IpcSender};

// Local modules
use crate::message::ProtocolMessage;
use crate::oplog::OpLog;

pub enum ParticipantState {
    Prepared,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    WaitingDecision,
};

pub struct Participant {
    id_str: String,
    log: oplog::OpLog,
    state: ParticipantState,
    exec_success_prob: f64,
    send_success_prob: f64,
    running: Arc<AtomicBool>,
    tx_to_coordinator: IpcSender<ProtocolMessage>,
    rx_from_coordinator: IpcReceiver<ProtocolMessage>,
};

impl Participant {
    ///
    /// Create a new Participant.
    ///
    /// Ensure it can communicate with the coordinator through ipc_channel.
    ///
    pub fn new(
        id_str: String,
        log: oplog::OpLog,
        exec_success_prob: f64,
        send_success_prob: f64,
        running: Arc<AtomicBool>,
        tx_to_coordinator: IpcSender<ProtocolMessage>,
        rx_from_coordinator: IpcReceiver<ProtocolMessage>) -> Participant {

            Participant {
                id_str,
                log,
                state: ParticipantState::Prepared,
                exec_success_prob,
                send_success_prob,
                running,
                tx_to_coordinator,
                rx_from_coordinator,
            }
        } 

    ///
    /// Mock a send operation. Current implementation is only a static check on a randomly generating float.
    ///
    pub fn send(&mut self) {
        let send_success: f64 = random();

        if send_success <= self.send_success_prob {
            // Success in sending
        } else {
            // Failed in sending
        }
    }

    ///
    /// Mock an execute operation. Current implementation is only a static check on a randomly generated float.
    ///
    pub fn execute(&mut self) {
        let exec_success: f64 = random();

        if exec_success <= self.exec_success_prob {
            // Success in executing
        } else {
            // Failed in execting
        }
    }

    ///
    /// Run the protocol, acting as a Participant.
    ///
    pub protocol(&mut self) {

    }
    
}