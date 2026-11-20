///
/// participant.rs
///
/// Implements participant of the 2PC Protocol
///
// Crates
extern crate rand;

// Rust libs
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use ipc_channel::TryRecvError;
use rand::random;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// Local modules
use crate::common::ProcessStatus;
use crate::message::{MessageType, ProtocolMessage};
use crate::oplog::OpLog;

pub enum ParticipantState {
    Prepared,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    WaitingDecision,
}

pub struct Participant {
    id_str: String,
    log: OpLog,
    state: ParticipantState,
    exec_success_prob: f64,
    send_success_prob: f64,
    running: Arc<AtomicBool>,
    tx_to_coordinator: IpcSender<ProtocolMessage>,
    rx_from_coordinator: IpcReceiver<ProtocolMessage>,
    num_committed: u64,
    num_aborted: u64,
    num_unknown: u64,
    pending: HashMap<u32, (MessageType, u32)>,
    completed: HashMap<u32, MessageType>,
}

impl ProcessStatus for Participant {
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

impl Participant {
    ///
    /// Create a new Participant.
    ///
    /// Ensure it can communicate with the coordinator through ipc_channel.
    ///
    pub fn new(
        id_str: String,
        log: OpLog,
        exec_success_prob: f64,
        send_success_prob: f64,
        running: Arc<AtomicBool>,
        tx_to_coordinator: IpcSender<ProtocolMessage>,
        rx_from_coordinator: IpcReceiver<ProtocolMessage>,
    ) -> Participant {
        Participant {
            id_str,
            log,
            state: ParticipantState::Prepared,
            exec_success_prob,
            send_success_prob,
            running,
            tx_to_coordinator,
            rx_from_coordinator,
            num_committed: 0,
            num_aborted: 0,
            num_unknown: 0,
            pending: HashMap::new(),
            completed: HashMap::new(),
        }
    }

    ///
    /// Run the protocol, acting as a Participant.
    ///
    pub fn protocol(&mut self) {
        // Receive a Request
        self.wait_on_coordinator_rx();

        self.num_unknown = self.pending.len() as u64;
        self.report_status();
    }

    ///
    /// Helper function to wait on RX from coordinator
    ///
    fn wait_on_coordinator_rx(&mut self) {
        loop {
            match self.rx_from_coordinator.try_recv() {
                Ok(res) => match res.mtype {
                    MessageType::CoordinatorPropose => {
                        self.attempt_proposed_op(&res);
                    }
                    MessageType::CoordinatorCommit => {
                        self.commit_proposed_op(&res);
                    }
                    MessageType::CoordinatorAbort => {
                        self.abort_proposed_op(&res);
                    }
                    MessageType::CoordinatorExit => break,
                    _ => {}
                },
                Err(TryRecvError::Empty) => {
                    // Channel is empty. Sleep for a bit and then retry.
                    thread::sleep(Duration::from_millis(10));
                }
                Err(TryRecvError::IpcError(err)) => {
                    log::debug!("Receive failed: {:?}", err);
                    break;
                }
            }
        }
    }

    ///
    /// Attempt an operation
    ///
    /// Used when we get a CoordinatorPropose Request from the Coordinator
    ///
    fn attempt_proposed_op(&mut self, proposal: &ProtocolMessage) -> bool {
        if self.completed.contains_key(&proposal.tx_id)
            || self.pending.contains_key(&proposal.tx_id)
        {
            return false;
        }
        self.state = ParticipantState::ReceivedP1;
        // Simulate an execute
        let exec_succeeded = self.execute();
        let vote_type = if exec_succeeded {
            MessageType::ParticipantVoteCommit
        } else {
            MessageType::ParticipantVoteAbort
        };

        // Record the vote to OpLog
        self.log.append(
            vote_type,
            proposal.tx_id,
            self.id_str.clone(),
            proposal.op_id,
        );

        self.state = if exec_succeeded {
            ParticipantState::VotedCommit
        } else {
            ParticipantState::VotedAbort
        };
        self.pending
            .insert(proposal.tx_id, (vote_type, proposal.op_id));

        // Simulate a send. If the send is dropped, it doesn't change the vote
        if self.send() {
            let vote = ProtocolMessage::generate(
                vote_type,
                proposal.tx_id,
                self.id_str.clone(),
                proposal.op_id,
            );

            if let Err(err) = self.tx_to_coordinator.send(vote) {
                eprintln!(
                    "Failed to send vote for transaction {}: {:?}",
                    proposal.tx_id, err
                );
            }
        }

        self.state = ParticipantState::WaitingDecision;
        exec_succeeded
    }

    ///
    /// Commit an operation
    ///
    /// Used when we get a CoordinatorCommit Request from the Coordinator
    ///
    fn commit_proposed_op(&mut self, decision: &ProtocolMessage) -> bool {
        if let Some(previous) = self.completed.get(&decision.tx_id) {
            assert_eq!(
                *previous,
                MessageType::CoordinatorCommit,
                "Conflicting transaction decision"
            );
            return false;
        }
        assert_eq!(
            self.pending.get(&decision.tx_id),
            Some(&(MessageType::ParticipantVoteCommit, decision.op_id)),
            "Cannot commit without voting commit",
        );
        self.log.append(
            MessageType::CoordinatorCommit,
            decision.tx_id,
            self.id_str.clone(),
            decision.op_id,
        );
        self.pending.remove(&decision.tx_id);
        self.completed
            .insert(decision.tx_id, MessageType::CoordinatorCommit);
        self.num_committed += 1;
        self.state = ParticipantState::Prepared;
        true
    }

    ///
    /// Abort an operation
    ///
    /// Used when we get a CoordinatorAbort Request from the Coordinator
    ///
    fn abort_proposed_op(&mut self, decision: &ProtocolMessage) -> bool {
        if let Some(previous) = self.completed.get(&decision.tx_id) {
            assert_eq!(
                *previous,
                MessageType::CoordinatorAbort,
                "Conflicting transaction decision"
            );
            return false;
        }
        if let Some((_, op_id)) = self.pending.get(&decision.tx_id) {
            assert_eq!(
                *op_id, decision.op_id,
                "Operation ID does not match proposal"
            );
        }
        self.log.append(
            MessageType::CoordinatorAbort,
            decision.tx_id,
            self.id_str.clone(),
            decision.op_id,
        );
        self.pending.remove(&decision.tx_id);
        self.completed
            .insert(decision.tx_id, MessageType::CoordinatorAbort);
        self.num_aborted += 1;
        self.state = ParticipantState::Prepared;
        true
    }

    ///
    /// Mock a send operation. Current implementation is only a static check on a randomly generating float.
    ///
    pub fn send(&mut self) -> bool {
        let send_success: f64 = random();

        send_success < self.send_success_prob
    }

    ///
    /// Mock an execute operation. Current implementation is only a static check on a randomly generated float.
    ///
    pub fn execute(&mut self) -> bool {
        let exec_success: f64 = random();

        exec_success < self.exec_success_prob
    }
}
