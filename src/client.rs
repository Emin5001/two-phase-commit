///
/// client.rs
///
/// Implements the Client of the 2PC Protocol
///

// Crates

// Rust libs
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool};
use std::sync::atomic::Ordering::{SeqCst};
use std::thread;
use std::time::Duration;

// Local modules
use crate::message::{ProtocolMessage, MessageType};
use crate::oplog::OpLog;
use crate::common::ProcessStatus;

pub enum ClientState {
    PreparedToSendTransaction,
    TransactionRequested,
    TransactionReceived,
};

pub struct Client {
    id_str: String,
    log: oplog::OpLog,
    num_requests: u32,
    running: Arc<AtomicBool>,
    tx_to_coordinator: IpcSender<ProtocolMessage>,
    rx_from_coordinator: IpcReceiver<ProtocolMessage>,
    num_committed: u64,
    num_aborted: u64,
    num_unknown: u64,
};

impl ProcessStatus for Client {
    fn id(&self) -> &str {
        &self.id_str
    }

    fn running(&self) -> &Arc<AtomicBool> {
        &self.running
    }

    fn counts(&self) -> (u64, u64, u64) {
        (self.num_committed, self.num_aborted, self.num_unknown)
    }
}

impl Client {
    ///
    /// Create a new Client.
    ///
    /// Ensure it can communicate with the coordinator through ipc_channel. 
    ///
    pub fn new(
        id_str: String,
        log: oplog::OpLog,
        num_requests: u32,
        running: Arc<AtomicBool>,
        tx_to_coordinator: IpcSender<ProtocolMessage>,
        rx_from_coordinator: IpcReceiver<ProtocolMessage>) -> Client {
            
            Client {
                id_str,
                log,
                num_requests,
                running,
                tx_to_coordinator,
                rx_from_coordinator,
                num_committed: 0,
                num_aborted: 0,
                num_unknown: 0
            }
        }
    
    ///
    /// Run the protocol, acting as a Client.
    ///
    pub fn protocol(&mut self) {

        self.wait_for_exit_signal();
        self.report_status();
    }

    ///
    /// Helper function to wait on a response from the Coordinator
    ///
    fn wait_on_coordinator_rx(&mut self) {
        // Wait as long as the program hasn't been Ctrl+c'd
        while self.running.load(SeqCst) {
            match self.rx_from_coordinator.try_recv() {
                Ok(res) => {
                    let rx_status = match res.mtype {
                        MessageType::ClientResultCommit => RequestStatus::Committed,
                        MessageType::ClientResultAbort => RequestStatus::Aborted,
                        _ => continue,
                    };

                    println!("Transaction {}: {:?}", res.tx_id, status);
                    // Do something with the result
                    break;
                },
                Err(_) => {
                    // TODO: only here for debug. Remove later on.
                    println!("Waiting...");
                }
            }
        }
    }

    ///
    /// Helper function to send a request to the Coordinator
    ///
    fn send_coordinator_tx(&mut self) {

    }
}