///
/// coordinator.rs
/// Implements coordinator of the 2PC Protocol
///
// Rust libs
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use ipc_channel::TryRecvError;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// Local modules
use crate::common::ProcessStatus;
use crate::message::{MessageType, ProtocolMessage};
use crate::oplog::OpLog;

struct Connection {
    id_str: String,
    tx: IpcSender<ProtocolMessage>,
    rx: IpcReceiver<ProtocolMessage>,
    connected: bool,
}

pub struct Coordinator {
    log: OpLog,
    running: Arc<AtomicBool>,
    clients: Vec<Connection>,
    participants: Vec<Connection>,
    completed: HashSet<u32>,
    num_committed: u64,
    num_aborted: u64,
}

impl ProcessStatus for Coordinator {
    fn id(&self) -> &str {
        "coordinator"
    }

    fn running(&self) -> &Arc<AtomicBool> {
        &self.running
    }

    fn counts(&self) -> (u64, u64, u64) {
        (self.num_committed, self.num_aborted, 0)
    }
}

impl Coordinator {
    ///
    /// Create a new Coordinator
    ///
    pub fn new(log: OpLog, running: Arc<AtomicBool>) -> Self {
        Self {
            log,
            running,
            clients: Vec::new(),
            participants: Vec::new(),
            completed: HashSet::new(),
            num_committed: 0,
            num_aborted: 0,
        }
    }

    pub fn add_client(
        &mut self,
        id_str: String,
        tx: IpcSender<ProtocolMessage>,
        rx: IpcReceiver<ProtocolMessage>,
    ) {
        self.clients.push(Connection {
            id_str,
            tx,
            rx,
            connected: true,
        });
    }

    pub fn add_participant(
        &mut self,
        id_str: String,
        tx: IpcSender<ProtocolMessage>,
        rx: IpcReceiver<ProtocolMessage>,
    ) {
        self.participants.push(Connection {
            id_str,
            tx,
            rx,
            connected: true,
        });
    }

    ///
    /// Run the protocol, acting as a Coordinator
    ///
    pub fn protocol(&mut self) {
        while self.running.load(SeqCst) {
            let mut received = false;
            for client_id in 0..self.clients.len() {
                if !self.running.load(SeqCst) {
                    break;
                }
                if !self.clients[client_id].connected {
                    continue;
                }
                match self.clients[client_id].rx.try_recv() {
                    Ok(request) => {
                        received = true;
                        if request.mtype == MessageType::ClientRequest
                            && request.sender_id == self.clients[client_id].id_str
                            && !self.completed.contains(&request.tx_id)
                        {
                            self.process_request(client_id, &request);
                        }
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::IpcError(err)) => {
                        log::warn!("Client disconnected: {:?}", err);
                        self.clients[client_id].connected = false;
                    }
                }
            }
            if !received {
                thread::sleep(Duration::from_millis(2));
            }
        }

        // Send exit after all decisions so children can finish their logs
        let exit =
            ProtocolMessage::generate(MessageType::CoordinatorExit, 0, self.id().to_string(), 0);
        for connection in self.participants.iter().chain(self.clients.iter()) {
            if let Err(err) = connection.tx.send(exit.clone()) {
                log::debug!("Failed to send exit to {}: {:?}", connection.id_str, err);
            }
        }
        self.report_status();
    }

    ///
    /// Propose a transaction and record the final decision
    ///
    fn process_request(&mut self, client_id: usize, request: &ProtocolMessage) {
        let proposal = ProtocolMessage::generate(
            MessageType::CoordinatorPropose,
            request.tx_id,
            self.id().to_string(),
            request.op_id,
        );
        self.log.append(
            proposal.mtype,
            proposal.tx_id,
            proposal.sender_id.clone(),
            proposal.op_id,
        );
        let mut can_commit = true;
        for participant in &mut self.participants {
            // Discard votes left over from earlier aborted transactions
            loop {
                match participant.rx.try_recv() {
                    Ok(_) => {}
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::IpcError(_)) => {
                        participant.connected = false;
                        break;
                    }
                }
            }
            if !participant.connected || participant.tx.send(proposal.clone()).is_err() {
                participant.connected = false;
                can_commit = false;
            }
        }

        // All participants must vote commit before the timeout
        let deadline = Instant::now() + Duration::from_millis(250);
        let mut voted = vec![false; self.participants.len()];
        while can_commit && voted.iter().any(|vote| !vote) {
            if !self.running.load(SeqCst) || Instant::now() >= deadline {
                can_commit = false;
                break;
            }
            for (participant, has_voted) in self.participants.iter_mut().zip(voted.iter_mut()) {
                if *has_voted {
                    continue;
                }
                match participant.rx.try_recv() {
                    Ok(vote) => {
                        if vote.tx_id != request.tx_id
                            || vote.op_id != request.op_id
                            || vote.sender_id != participant.id_str
                        {
                            continue;
                        }
                        match vote.mtype {
                            MessageType::ParticipantVoteCommit => *has_voted = true,
                            MessageType::ParticipantVoteAbort => {
                                can_commit = false;
                                break;
                            }
                            _ => {}
                        }
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::IpcError(err)) => {
                        log::warn!("Participant disconnected: {:?}", err);
                        participant.connected = false;
                        can_commit = false;
                        break;
                    }
                }
            }
            if can_commit && voted.iter().any(|vote| !vote) {
                thread::sleep(Duration::from_millis(2));
            }
        }

        let (decision_type, result_type) = if can_commit {
            self.num_committed += 1;
            (
                MessageType::CoordinatorCommit,
                MessageType::ClientResultCommit,
            )
        } else {
            self.num_aborted += 1;
            (
                MessageType::CoordinatorAbort,
                MessageType::ClientResultAbort,
            )
        };
        self.log.append(
            decision_type,
            request.tx_id,
            self.id().to_string(),
            request.op_id,
        );
        self.completed.insert(request.tx_id);
        let decision = ProtocolMessage::generate(
            decision_type,
            request.tx_id,
            self.id().to_string(),
            request.op_id,
        );
        for participant in &mut self.participants {
            if let Err(err) = participant.tx.send(decision.clone()) {
                log::warn!(
                    "Failed to send decision to {}: {:?}",
                    participant.id_str,
                    err
                );
                participant.connected = false;
            }
        }
        let result = ProtocolMessage::generate(
            result_type,
            request.tx_id,
            self.id().to_string(),
            request.op_id,
        );
        if let Err(err) = self.clients[client_id].tx.send(result) {
            log::warn!("Failed to send client result: {:?}", err);
            self.clients[client_id].connected = false;
        }
    }
}
