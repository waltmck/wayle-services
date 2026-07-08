use crate::{core::notification::Notification, types::ClosedReason};

#[derive(Clone)]
pub(crate) enum NotificationEvent {
    Add(Box<Notification>),
    Remove(u32, ClosedReason),
    /// Batch removal of several notifications in one atomic pass ("clear all" / clearing
    /// a group), so the store delete and the reactive list update happen once instead of
    /// once per notification.
    RemoveMany(Vec<u32>, ClosedReason),
}
