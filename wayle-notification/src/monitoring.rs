use std::{collections::HashSet, sync::Arc, time::Duration};

use chrono::Utc;
use futures::StreamExt;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};
use wayle_core::Property;
use wayle_traits::ServiceMonitoring;
use zbus::{Connection, fdo::DBusProxy};

use crate::{
    core::notification::Notification,
    error::Error,
    events::NotificationEvent,
    persistence::NotificationStore,
    popup_timer::PopupTimerManager,
    service::NotificationService,
    types::{
        ClosedReason, Signal,
        dbus::{SERVICE_INTERFACE, SERVICE_PATH},
    },
};

impl ServiceMonitoring for NotificationService {
    type Error = Error;
    #[instrument(skip_all, err)]
    async fn start_monitoring(&self) -> Result<(), Self::Error> {
        handle_notifications(self).await?;
        Ok(())
    }
}

#[instrument(skip_all)]
async fn handle_notifications(service: &NotificationService) -> Result<(), Error> {
    let mut event_receiver = service.notif_tx.subscribe();
    let notification_list = service.notifications.clone();
    let popup_list = service.popups.clone();
    let popup_dur = service.popup_duration.clone();
    let dnd = service.dnd.clone();
    let store = service.store.clone();
    let cancellation_token = service.cancellation_token.clone();
    let remove_expired = service.remove_expired.clone();
    let connection = service.connection.clone();
    let notif_tx = service.notif_tx.clone();
    let popup_timers = service.popup_timers.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Notification monitoring cancelled, stopping");
                    return;
                }
                Ok(event) = event_receiver.recv() => {
                    match event {
                        NotificationEvent::Add(notif) => {
                            handle_notification_added(
                                &notif,
                                &notification_list,
                                &store,
                                &remove_expired,
                                &notif_tx
                            );
                            handle_popup_added(
                                &notif,
                                &popup_list,
                                &popup_dur,
                                dnd.clone(),
                                &popup_timers,
                            );
                        }
                        NotificationEvent::Remove(id, reason) => {
                            handle_notification_removed(
                                id,
                                reason,
                                &notification_list,
                                &popup_list,
                                &store,
                                &connection,
                                &popup_timers,
                            ).await;
                        }
                    }
                }
            }
        }
    });

    spawn_owner_watching(
        service.notifications.clone(),
        service.popups.clone(),
        service.connection.clone(),
        service.cancellation_token.clone(),
    );

    Ok(())
}

/// Removes a notification's actions in place so it is no longer clickable.
///
/// `default_action` is cleared before `actions` so an observer watching only `actions`
/// still sees a consistent (action-less) state by the time it reacts.
fn strip_actions(notif: &Notification) {
    notif.default_action.set(None);
    notif.actions.set(vec![]);
}

/// Reactively strips a notification's actions once its owning D-Bus connection is gone
/// (the app can no longer service them). Fully signal-driven: subscribe to
/// `NameOwnerChanged`, seed once from the current bus names, then react to removals.
/// No polling.
fn spawn_owner_watching(
    notifications: Property<Vec<Arc<Notification>>>,
    popups: Property<Vec<Arc<Notification>>>,
    connection: Connection,
    cancellation_token: CancellationToken,
) {
    tokio::spawn(async move {
        let Ok(dbus_proxy) = DBusProxy::new(&connection).await else {
            warn!("cannot create DBus proxy for notification owner watching");
            return;
        };

        // Subscribe before seeding so no disconnect is missed in the gap between them.
        let Ok(mut name_owner_changed) = dbus_proxy.receive_name_owner_changed().await else {
            warn!("cannot subscribe to NameOwnerChanged for notification owner watching");
            return;
        };

        // Seed: strip notifications/popups whose owner is unknown or no longer on the bus
        // (restored from a prior session, or an app that exited while we were down).
        match dbus_proxy.list_names().await {
            Ok(names) => {
                let live: HashSet<String> =
                    names.into_iter().map(|name| name.to_string()).collect();
                let notifs = notifications.get();
                let pops = popups.get();
                for notif in notifs.iter().chain(pops.iter()) {
                    if !notif.owner.get().is_some_and(|owner| live.contains(&owner)) {
                        strip_actions(notif);
                    }
                }
            }
            Err(err) => {
                warn!(error = %err, "cannot list D-Bus names to reconcile notification owners");
            }
        }

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => return,
                signal = name_owner_changed.next() => {
                    let Some(signal) = signal else { return };
                    let Ok(args) = signal.args() else { continue };

                    let disconnected = args.old_owner().is_some() && args.new_owner().is_none();
                    if !disconnected {
                        continue;
                    }

                    let vanished = args.name().to_string();
                    let notifs = notifications.get();
                    let pops = popups.get();
                    for notif in notifs.iter().chain(pops.iter()) {
                        if notif.owner.get().as_deref() == Some(vanished.as_str()) {
                            debug!(id = notif.id, owner = %vanished, "owner disconnected, stripping actions");
                            strip_actions(notif);
                        }
                    }
                }
            }
        }
    });
}

fn handle_popup_added(
    incoming_popup: &Notification,
    popups: &Property<Vec<Arc<Notification>>>,
    popup_duration: &Property<u32>,
    dnd: Property<bool>,
    popup_timers: &Arc<PopupTimerManager>,
) {
    if dnd.get() {
        return;
    }

    let mut list = popups.get();

    // Stable identity (mirrors the history list): update an existing popup's Property
    // fields in place instead of replacing its Arc, so the popup card reacts to the
    // change rather than being left bound to a stale Arc.
    let popup = match list.iter().find(|popup| popup.id == incoming_popup.id).cloned() {
        Some(existing) => {
            existing.update_from(incoming_popup);
            existing.owner.set(incoming_popup.owner.get());
            existing
        }
        None => Arc::new(incoming_popup.clone()),
    };

    list.retain(|p| p.id != popup.id);
    list.insert(0, popup.clone());
    popups.replace(list);

    let default_duration = Duration::from_millis(popup_duration.get() as u64);

    match popup.expire_timeout.get() {
        Some(0) => {}
        Some(ttl) => {
            let expire = Duration::from_millis(ttl as u64);
            popup_timers.start(popup.id, default_duration.min(expire));
        }
        None => {
            popup_timers.start(popup.id, default_duration);
        }
    }
}

fn handle_notification_added(
    incoming_notif: &Notification,
    notifications: &Property<Vec<Arc<Notification>>>,
    store: &Option<NotificationStore>,
    remove_expired: &Property<bool>,
    notif_tx: &broadcast::Sender<NotificationEvent>,
) {
    let mut list = notifications.get();

    // Transient notifications are not kept in history. If a notification is flipped to
    // transient over an existing id, drop the stale history entry so it leaves history.
    if incoming_notif.is_transient.get() {
        if list.iter().any(|notif| notif.id == incoming_notif.id) {
            list.retain(|notif| notif.id != incoming_notif.id);
            notifications.replace(list);
            if let Some(store) = store.as_ref() {
                let _ = store.remove(incoming_notif.id);
            }
        }
        return;
    }

    // Stable identity: update an existing notification in place (observers react via its
    // Property fields) rather than replacing its Arc; only mint a new Arc for a new id.
    let notif_arc = match list.iter().find(|notif| notif.id == incoming_notif.id).cloned() {
        Some(existing) => {
            existing.update_from(incoming_notif);
            // Refresh the owning connection (e.g. an app that reconnected and replaced
            // its own notification) so directed signals reach the live client.
            existing.owner.set(incoming_notif.owner.get());
            debug!(
                id = existing.id,
                app = ?existing.app_name.get(),
                "updating existing notification in place"
            );
            existing
        }
        None => {
            debug!(
                id = incoming_notif.id,
                app = ?incoming_notif.app_name.get(),
                summary = %incoming_notif.summary.get(),
                list_size = list.len(),
                "adding new notification"
            );
            Arc::new(incoming_notif.clone())
        }
    };

    // Move to (or keep at) the front, preserving the Arc identity.
    list.retain(|notif| notif.id != notif_arc.id);
    list.insert(0, notif_arc.clone());
    notifications.replace(list);

    if let Some(store) = store.as_ref() {
        let _ = store.add(incoming_notif);
    };

    if !remove_expired.get() {
        return;
    }

    let Some(ttl) = notif_arc.expire_timeout.get() else {
        return;
    };

    let expiration_time = notif_arc.timestamp.get() + Duration::from_millis(ttl as u64);
    let now = Utc::now();

    if expiration_time <= now {
        let mut list = notifications.get();
        list.retain(|notif| notif.id != notif_arc.id);
        notifications.set(list);
        return;
    }

    let time_until_expiration = (expiration_time - now).to_std().unwrap_or(Duration::ZERO);
    let id = notif_arc.id;
    let tx = notif_tx.clone();

    tokio::spawn(async move {
        tokio::time::sleep(time_until_expiration).await;
        let _ = tx.send(NotificationEvent::Remove(id, ClosedReason::Expired));
    });
}

async fn handle_notification_removed(
    id: u32,
    reason: ClosedReason,
    notifications: &Property<Vec<Arc<Notification>>>,
    popups: &Property<Vec<Arc<Notification>>>,
    store: &Option<NotificationStore>,
    connection: &Connection,
    popup_timers: &Arc<PopupTimerManager>,
) {
    if !matches!(reason, ClosedReason::Expired) {
        popup_timers.cancel(id);

        let mut popup_list = popups.get();
        popup_list.retain(|popup| popup.id != id);
        popups.set(popup_list);
    }

    let mut notif_list = notifications.get();
    let prev_len = notif_list.len();
    // Capture the owner before removing the notification from the list, so the close
    // signal can be directed to the creating connection.
    let owner = notif_list
        .iter()
        .find(|notif| notif.id == id)
        .and_then(|notif| notif.owner.get());
    notif_list.retain(|notif| notif.id != id);

    if notif_list.len() == prev_len {
        return;
    }

    notifications.set(notif_list);

    if let Some(store) = store.as_ref() {
        let _ = store.remove(id);
    };

    // Direct the close signal to the owning connection only; skip if unknown, for the
    // same reason as ActionInvoked (a broadcast reaches clients that don't filter by id).
    let Some(owner) = owner else {
        return;
    };

    debug!(id = id, ?reason, "emitting NotificationClosed");
    if let Err(err) = connection
        .emit_signal(
            Some(owner.as_str()),
            SERVICE_PATH,
            SERVICE_INTERFACE,
            Signal::NotificationClosed.as_str(),
            &(id, reason as u32),
        )
        .await
    {
        warn!(id = id, error = %err, "cannot emit NotificationClosed signal");
    }
}
