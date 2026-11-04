use std::fmt::Display;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct SyncStatus {
    pub initial_seqno: u32,
    pub full_synced_seqno: u32,
    pub full_synced_seqno_utime: u32,
    pub last_seen_seqno: u32,
    pub last_seen_seqno_utime: u32,
    pub sync_utime: u32,
}

impl Display for SyncStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = format!(
            "full_synced_seqno: {}, last_seen_seqno: {})",
            self.full_synced_seqno, self.last_seen_seqno
        );
        write!(f, "{str}")
    }
}
