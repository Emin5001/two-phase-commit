///
/// client.rs
///
/// Implements the Client of the 2PC Protocol
///

// Crates

// Rust libs
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use std::sync::{Arc}
use std::sync::atomic::{AtomicBool}
use std::sync::atomic::Ordering::{SeqCst}
use std::thread;
use std::time::Duration;

// Local modules
use crate::message::{ProtocolMessage, MessageType};
use crate::oplog::OpLog;

pub enum ClientState {
    PreparedToSendTransaction,
    TransactionRequested,
    TransactionReceived,
};

pub struct Client {
    log: oplog::OpLog,
    num_requests: u32,
    running: Arc<AtomicBool>,
    tx_to_coordinator: IpcSender<ProtocolMessage>,
    rx_from_coordinator: IpcReceiver<ProtocolMessage>,
};

impl Client {
    ///
    /// Create a new Client.
    ///
    /// Ensure it can communicate with the coordinator through ipc_channel. 
    ///
    pub fn new(
        log: oplog::OpLog,
        num_requests: u32,
        running: Arc<AtomicBool>,
        tx_to_coordinator: IpcSender<ProtocolMessage>,
        rx_from_coordinator: IpcReceiver<ProtocolMessage>) -> Client {
            
            Client {
                log,
                num_requests,
                running,
                tx_to_coordinator,
                rx_from_coordinator,
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


    ///
    /// Helper function to make sure the client can see the ctrl+c to exit.
    ///
    fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        while self.running.load(SeqCst) {
            thread::sleep(Duration::from_millis(10));
        }

        trace!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// Helper function to report the abort/commit/unknown aggregate status of all 
    /// transaction requests made by this Client before exiting.
    ///
    fn report_status(&mut self) {
        // TODO: Actually track properly when implementing protocol
        let successful_ops: u64 = 0;
        let failed_ops: u64 = 0;
        let unknown_ops: u64 = 0;

        println!(
            "{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}",
            self.id_str.clone(),
            successful_ops,
            failed_ops,
            unknown_ops);
    }
}