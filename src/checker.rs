//!
//! checker.rs
//! Tools for checking output logs produced by the _T_wo _P_hase _C_ommit
//! project in run mode. Exports a single public function called check_last_run
//! that accepts a directory where client, participant, and coordinator log
//! files are found, and the number of clients, participants. Loads and analyses
//! log files to check a handful of correctness invariants.
//!
extern crate clap;
extern crate ctrlc;
extern crate log;
extern crate stderrlog;

use std::collections::HashMap;

use crate::message;
use crate::message::MessageType;
use crate::message::ProtocolMessage;
use crate::oplog::OpLog;

///
/// check_participant()
///
/// Given a participant name and HashMaps that represents the log files for the
/// participant and coordinator (already filtered for commit records), check
/// that the committed and aborted transactions are agreed upon by the two.
///
/// <params>
///     participant: name of participant (label)
///     ncommit: number of committed transactions from coordinator
///     nabort: number of aborted transactions from coordinator
///     ccommitted: map of committed transactions from coordinator
///     plog: map of participant operations
///
fn check_participant(
    participant: &str,
    num_commit: usize,
    num_abort: usize,
    coord_committed: &HashMap<u32, ProtocolMessage>,
    coord_aborted: &HashMap<u32, ProtocolMessage>,
    participant_log: &HashMap<u32, ProtocolMessage>,
) -> bool {
    let mut result = true;

    // Filter the participant log for Global Commits, Local Commits, and Aborted
    let participant_commit_map: HashMap<u32, message::ProtocolMessage> = participant_log
        .iter()
        .filter(|e| (*e.1).mtype == MessageType::CoordinatorCommit)
        .map(|(k, v)| (*k, v.clone()))
        .collect();
    let participant_local_commit_map: HashMap<u32, message::ProtocolMessage> = participant_log
        .iter()
        .filter(|e| (*e.1).mtype == MessageType::ParticipantVoteCommit)
        .map(|(k, v)| (*k, v.clone()))
        .collect();
    let participant_abort_map: HashMap<u32, message::ProtocolMessage> = participant_log
        .iter()
        .filter(|e| (*e.1).mtype == MessageType::CoordinatorAbort)
        .map(|(k, v)| (*k, v.clone()))
        .collect();

    let num_participant_commit = participant_commit_map.len();
    let num_participant_local_commit = participant_local_commit_map.len();
    let num_participant_abort = participant_abort_map.len();

    result &= num_participant_commit <= num_commit;
    result &= num_participant_local_commit >= num_commit;
    result &= num_participant_abort <= num_abort;

    assert!(num_participant_commit <= num_commit);
    assert!(num_commit <= num_participant_local_commit);
    assert!(num_abort >= num_participant_abort);

    for (_, coord_msg) in coord_committed.iter() {
        let txid = coord_msg.tx_id;
        let mut _found_txid = 0;
        let mut found_local_txid = 0;
        for (_, participant_msg) in participant_commit_map.iter() {
            if participant_msg.tx_id == txid && participant_msg.op_id == coord_msg.op_id {
                _found_txid += 1;
            }
        }

        for (_, participant_msg) in participant_local_commit_map.iter() {
            // Handle the case where the participant simply doesn't get the
            // global commit message from the coordinator. If the coordinator
            // committed the transaction, the participant has to have voted in
            // favor. Namely, when _found_txid != found_local_txid.
            if participant_msg.tx_id == txid && participant_msg.op_id == coord_msg.op_id {
                found_local_txid += 1;
            }
        }

        // Exactly one commit of txid per participant
        assert!(_found_txid <= 1, "Duplicate participant commit");
        result &= found_local_txid == 1;
        assert!(found_local_txid == 1);
    }
    // Check every participant decision against the coordinator's decision
    let mut decided = std::collections::HashSet::new();
    for decision in participant_commit_map
        .values()
        .chain(participant_abort_map.values())
    {
        assert!(
            decided.insert(decision.tx_id),
            "Duplicate participant decision"
        );
        let global = if decision.mtype == MessageType::CoordinatorCommit {
            coord_committed
        } else {
            coord_aborted
        };
        assert!(
            global
                .values()
                .any(|message| message.tx_id == decision.tx_id && message.op_id == decision.op_id),
            "Participant decision does not match coordinator"
        );
    }
    let mut voted = std::collections::HashSet::new();
    for vote in participant_log.values().filter(|message| {
        matches!(
            message.mtype,
            MessageType::ParticipantVoteCommit | MessageType::ParticipantVoteAbort
        )
    }) {
        assert!(voted.insert(vote.tx_id), "Duplicate participant vote");
    }
    let relation = if num_participant_commit == num_commit {
        "=="
    } else {
        "<="
    };
    println!(
        "{} OK: C:{} {} {}(C-global), A:{} <= {}(A-global)",
        participant, num_participant_commit, relation, num_commit, num_participant_abort, num_abort
    );
    result
}

///
/// check_last_run()
///
/// Accepts a directory where client, participant, and coordinator log files are
/// found, and the number of clients, participants. Loads and analyses log files
/// to check a handful of correctness invariants.
///
/// <params>
///     num_clients: Number of clients
///     num_requests: Number of requests per client
///     num_participants: Number of participants
///     log_path: Directory for client, participant, and coordinator logs
///
pub fn check_last_run(
    num_clients: u32,
    num_requests: u32,
    num_participants: u32,
    log_path: &String,
) {
    log::info!(
        "Checking 2PC run:  {} requests * {} clients, {} participants",
        num_requests,
        num_clients,
        num_participants
    );

    let coord_log_path = format!("{}//{}", log_path, "coordinator.log");
    let coord_log = OpLog::from_file(coord_log_path);

    let lock = coord_log.arc();
    let coord_map = lock.lock().unwrap();

    // Filter coordinator logs for Commit and Abort
    let committed: HashMap<u32, message::ProtocolMessage> = coord_map
        .iter()
        .filter(|e| (*e.1).mtype == MessageType::CoordinatorCommit)
        .map(|(k, v)| (*k, v.clone()))
        .collect();
    let aborted: HashMap<u32, message::ProtocolMessage> = coord_map
        .iter()
        .filter(|e| (*e.1).mtype == MessageType::CoordinatorAbort)
        .map(|(k, v)| (*k, v.clone()))
        .collect();

    let num_commit = committed.len();
    let num_abort = aborted.len();

    // Check that each transaction has at most one final decision
    let mut decided = std::collections::HashSet::new();
    for decision in committed.values().chain(aborted.values()) {
        assert!(
            decided.insert(decision.tx_id),
            "Duplicate coordinator decision"
        );
        assert!(
            decision.tx_id < num_clients * num_requests,
            "Transaction ID outside configured range"
        );
    }
    assert!(num_commit + num_abort <= (num_clients as usize) * (num_requests as usize));

    // Iterate and check each participant
    for pid in 0..num_participants {
        let participant_id_str = format!("participant_{}", pid);
        let participant_log_path = format!("{}//{}.log", log_path, participant_id_str);
        let participant_oplog = OpLog::from_file(participant_log_path);
        let participant_lock = participant_oplog.arc();
        let participant_log = participant_lock.lock().unwrap();
        assert!(check_participant(
            &participant_id_str,
            num_commit,
            num_abort,
            &committed,
            &aborted,
            &participant_log
        ));
    }
}
