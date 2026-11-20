///
/// client.rs
///
/// Implements the Client of the 2PC Protocol
///
// Crates

// Rust libs
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use ipc_channel::TryRecvError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// Local modules
use crate::common::ProcessStatus;
use crate::message::{MessageType, ProtocolMessage, RequestStatus};
use crate::oplog::OpLog;

pub enum ClientState {
    PreparedToSendTransaction,
    TransactionRequested,
    TransactionReceived,
}

pub struct Client {
    id_str: String,
    unique_id: u32, // We can have multiple Clients. Each client will have a unique ID that represents that Client.
    log: OpLog,
    num_requests: u32,
    running: Arc<AtomicBool>,
    tx_to_coordinator: IpcSender<ProtocolMessage>,
    rx_from_coordinator: IpcReceiver<ProtocolMessage>,
    num_committed: u64,
    num_aborted: u64,
    num_unknown: u64,
}

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
        unique_id: u32,
        log: OpLog,
        num_requests: u32,
        running: Arc<AtomicBool>,
        tx_to_coordinator: IpcSender<ProtocolMessage>,
        rx_from_coordinator: IpcReceiver<ProtocolMessage>,
    ) -> Client {
        Client {
            id_str,
            unique_id,
            log,
            num_requests,
            running,
            tx_to_coordinator,
            rx_from_coordinator,
            num_committed: 0,
            num_aborted: 0,
            num_unknown: 0,
        }
    }

    ///
    /// Run the protocol, acting as a Client.
    ///
    pub fn protocol(&mut self) {
        let mut connected = true;
        // Send num_requests amount of requests to the Coordinator
        for client_tx_id in 0..self.num_requests {
            // If Ctrl+C is pressed while we are running the protocol, Exit.
            if !self.running.load(SeqCst) {
                break;
            }

            // Make tx_id unique, as we can have multiple Clients
            let tx_id = self.unique_id * self.num_requests + client_tx_id;

            let message = ProtocolMessage::generate(
                MessageType::ClientRequest,
                tx_id,
                self.id_str.clone(),
                client_tx_id,
            );

            if let Err(err) = self.tx_to_coordinator.send(message) {
                eprintln!("Failed to send request: {:?}", err);
                connected = false;
                break;
            }

            if !self.wait_on_coordinator_rx(tx_id) {
                connected = false;
                break;
            }
        }

        // Wait for exit
        if connected {
            loop {
                match self.rx_from_coordinator.try_recv() {
                    Ok(message) if message.mtype == MessageType::CoordinatorExit => break,
                    Ok(_) => {}
                    Err(TryRecvError::Empty) => thread::sleep(Duration::from_millis(10)),
                    Err(TryRecvError::IpcError(_)) => break,
                }
            }
        }
        self.report_status();
    }

    ///
    /// Helper function to wait on a RX from the Coordinator
    ///
    fn wait_on_coordinator_rx(&mut self, tx_id: u32) -> bool {
        // Wait for a decision or coordinator shutdown
        loop {
            match self.rx_from_coordinator.try_recv() {
                Ok(res) => {
                    if res.mtype == MessageType::CoordinatorExit {
                        self.num_unknown += 1;
                        return false;
                    }
                    if res.tx_id != tx_id {
                        continue;
                    }
                    let rx_status = match res.mtype {
                        MessageType::ClientResultCommit => RequestStatus::Committed,
                        MessageType::ClientResultAbort => RequestStatus::Aborted,
                        _ => continue,
                    };
                    log::debug!("Transaction {}: {:?}", res.tx_id, rx_status);
                    self.log
                        .append(res.mtype, res.tx_id, self.id_str.clone(), res.op_id);

                    // Update counters
                    match rx_status {
                        RequestStatus::Committed => self.num_committed += 1,
                        RequestStatus::Aborted => self.num_aborted += 1,
                        RequestStatus::Unknown => self.num_unknown += 1,
                    }
                    return true;
                }
                Err(TryRecvError::Empty) => {
                    // Channel is empty. Sleep for a bit and then retry.
                    thread::sleep(Duration::from_millis(10));
                }
                Err(TryRecvError::IpcError(err)) => {
                    log::warn!("Receive failed: {:?}", err);
                    self.num_unknown += 1;
                    return false;
                }
            }
        }
    }
}
