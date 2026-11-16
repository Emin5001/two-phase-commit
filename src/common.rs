///
/// common.rs
///
/// Defines shared data, such as Traits
///

// Crates

// Rust libs
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool};
use std::sync::atomic::Ordering::{SeqCst};
use std::thread;
use std::time::Duration;

// Local modules

pub trait ProcessStatus {
    fn id(&self) -> &str;
    fn running(&self) -> &Arc<AtomicBool>;
    fn counts(&self) -> (u64, u64, u64);

    fn wait_for_exit_signal(&self) {
        trace!("{}::Waiting for exit signal", self.id());

        while self.running().load(SeqCst) {
            thread::sleep(Duration::from_millis(10));
        }

        trace!("{}::Exiting", self.id());
    }

    fn report_status(self) {
        let (committed, aborted, unknown) = self.counts();

        println!(
            "{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}",
            self.id(),
            committed,
            aborted,
            unknown);
    }
}