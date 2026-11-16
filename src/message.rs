//!
//! message.rs
//! Defines the messages used for communicating between nodes
//!
extern crate serde;
extern crate serde_json;

use std::sync::atomic::{AtomicU32, Ordering};

/// What type of Message we are sending on the channel between processes.
#[derive(serde::Seralize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageType {
    CoordinatorPropose,    // Coordinator proposes an update to Participants
    CoordinatorAbort,      // Coordinator decides to Rollback
    CoordinatorCommit,     // Coordinator decides to Commit
    ParticipantVoteCommit, // Vote to Commit
    ParticipantVoteAbort,  // Vote to Abort
    ClientRequest,         // Request from Client
    ClientResultCommit,    // Request from Client has been Committed
    ClientResultAbort,     // Request from Client has not been Committed and has been Aborted
}

/// Result of the Clients Request.
#[derive(serde::Seralize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestStatus {
    Committed,
    Aborted,
    Unknown, // Usually binary. Unknown just for the heck of it.
}

#[derive(serde::Seralize, serde::Deserialize, Clone, Debug)]
pub struct ProtocolMessage {
    pub mtype: MessageType, // Type of message we send
    pub uid: u32,           // The unique ID relative to the current process
    pub tx_id: u32,         // The unique transaction ID that the client requested
    pub sender_id: String,  // The unique sender ID that represents what process sent this ProtocolMessage
    pub op_id: u32,         // Operation ID, relative to the original client who started this transaction.
}

static COUNTER: AtomicU32 = AtomicU32::new(1);

impl ProtocolMessage {
    pub fn generate(
        mtype: MessageType,
        tx_id: u32,
        sender_id: String,
        op_id: u32) -> ProtocolMessage {
        ProtocolMessage {
            mtype,
            uid: COUNTER.fetch_add(1, Ordering::SeqCst),
            tx_id,
            sender_id,
            op_id,
        }
    }
}
