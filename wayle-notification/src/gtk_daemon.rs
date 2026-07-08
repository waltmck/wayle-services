use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use chrono::Utc;
use derive_more::Debug;
use tokio::sync::broadcast;
use tracing::{debug, instrument};
use wayle_core::Property;
use zbus::{
    Connection, fdo,
    zvariant::{OwnedValue, Str, Value},
};

use crate::{
    core::{
        notification::Notification,
        types::{GtkAction, GtkDispatch, NotificationHints, NotificationProps, NotificationSource},
    },
    daemon::IdCounter,
    desktop_entry,
    events::NotificationEvent,
    glob, image_cache,
    types::{ClosedReason, Urgency},
};

/// The `"app."` prefix on GNotification detailed action names, stripped before dispatch
/// via `org.freedesktop.Application.ActivateAction`.
const APP_ACTION_PREFIX: &str = "app.";

/// Implements `org.gtk.Notifications`, the protocol GLib/GTK apps use via `GNotification`
/// (`g_application_send_notification`). Notifications are funneled into the same
/// `NotificationEvent` pipeline as freedesktop ones, so DND, popups, history, expiry and
/// persistence are shared; only action dispatch differs (see [`NotificationSource`]).
#[derive(Debug)]
pub(crate) struct GtkNotificationsDaemon {
    #[debug(skip)]
    pub id_counter: Arc<IdCounter>,
    #[debug(skip)]
    pub zbus_connection: Connection,
    #[debug(skip)]
    pub notif_tx: broadcast::Sender<NotificationEvent>,
    #[debug(skip)]
    pub blocklist: Property<Vec<String>>,
    /// Maps `(app_id, notification_id)` → the synthesized `u32` id, so a re-send with the
    /// same key replaces in place and a `RemoveNotification` can find the id.
    #[debug(skip)]
    pub keys: Mutex<HashMap<(String, String), u32>>,
}

#[zbus::interface(name = "org.gtk.Notifications")]
impl GtkNotificationsDaemon {
    /// Adds (or replaces, if the `(app_id, notification_id)` key already exists) a GTK
    /// notification.
    #[instrument(
        skip(self, notification),
        fields(app_id = %app_id, gtk_id = %notification_id)
    )]
    pub async fn add_notification(
        &self,
        app_id: String,
        notification_id: String,
        notification: HashMap<String, OwnedValue>,
    ) -> fdo::Result<()> {
        // Resolve a human-readable app name for display and for the blocklist, so GTK
        // notifications share the freedesktop human-name key space. Fall back to app id.
        let app_name =
            desktop_entry::resolve_name(&app_id).unwrap_or_else(|| app_id.clone());

        let blocked = self
            .blocklist
            .get()
            .iter()
            .any(|pattern| glob::matches(pattern, &app_name));
        if blocked {
            debug!(app = %app_name, "gtk notification blocked by blocklist");
            return Ok(());
        }

        let parsed = ParsedGtkNotification::from_vardict(&app_id, &notification_id, &notification);
        let id = self.resolve_id(&app_id, &notification_id);

        debug!(id, app = %app_name, "adding gtk notification");

        let notif = Notification::new(
            NotificationProps {
                id,
                app_name,
                replaces_id: 0,
                app_icon: parsed.app_icon.unwrap_or_default(),
                summary: parsed.title,
                body: parsed.body.unwrap_or_default(),
                actions: parsed.actions_flat,
                hints: parsed.hints,
                // GTK notifications carry no timeout; keep them in history until removed.
                expire_timeout: -1,
                timestamp: Utc::now(),
                owner: None,
                source: NotificationSource::Gtk(parsed.dispatch),
            },
            self.zbus_connection.clone(),
            self.notif_tx.clone(),
        );

        let _ = self.notif_tx.send(NotificationEvent::Add(Box::new(notif)));
        Ok(())
    }

    /// Withdraws a notification previously added with the same key.
    #[instrument(skip(self), fields(app_id = %app_id, gtk_id = %notification_id))]
    pub async fn remove_notification(
        &self,
        app_id: String,
        notification_id: String,
    ) -> fdo::Result<()> {
        if let Some(id) = self.take_id(&app_id, &notification_id) {
            let _ = self
                .notif_tx
                .send(NotificationEvent::Remove(id, ClosedReason::Closed));
        }
        Ok(())
    }
}

impl GtkNotificationsDaemon {
    /// Reuses the `u32` id for an existing key (so a re-send replaces in place); otherwise
    /// allocates a fresh id from the shared counter and records the mapping.
    fn resolve_id(&self, app_id: &str, gtk_id: &str) -> u32 {
        let key = (app_id.to_owned(), gtk_id.to_owned());
        let mut keys = self.keys.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(&id) = keys.get(&key) {
            return id;
        }
        let id = self.id_counter.next_id();
        keys.insert(key, id);
        id
    }

    fn take_id(&self, app_id: &str, gtk_id: &str) -> Option<u32> {
        let key = (app_id.to_owned(), gtk_id.to_owned());
        self.keys
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&key)
    }
}

/// The pieces of an incoming `a{sv}` GTK notification that map onto our model.
struct ParsedGtkNotification {
    title: String,
    body: Option<String>,
    /// Themed icon name (drives `app_icon`), if the icon was a themed GIcon.
    app_icon: Option<String>,
    /// Synthetic hints carrying urgency (+ image-path / desktop-entry) so the shared
    /// `from_props` path derives urgency, icon file and desktop entry as usual.
    hints: NotificationHints,
    /// Flat `[id, label, ...]` of the buttons for display (`id` = `"app."`-prefixed name).
    actions_flat: Vec<String>,
    dispatch: GtkDispatch,
}

impl ParsedGtkNotification {
    fn from_vardict(app_id: &str, gtk_id: &str, map: &HashMap<String, OwnedValue>) -> Self {
        let title = owned_string(map.get("title")).unwrap_or_default();
        let body = owned_string(map.get("body"));

        // Collapse GNotification's 4 priorities onto fdo's 3 urgency levels: only
        // `urgent` is Critical; `high` (and `normal`) map to Normal, `low` to Low.
        let urgency = match owned_string(map.get("priority")).as_deref() {
            Some("urgent") => Urgency::Critical,
            Some("low") => Urgency::Low,
            _ => Urgency::Normal,
        };

        let (app_icon, image_path) = map
            .get("icon")
            .map(parse_icon)
            .unwrap_or((None, None));

        // Buttons → displayable actions + dispatch metadata. Only `"app."`-prefixed
        // actions are dispatchable (matching GNOME); others are ignored.
        let mut actions_flat = Vec::new();
        let mut button_actions = HashMap::new();
        for button in parse_buttons(map.get("buttons")) {
            let Some(name) = button.action.strip_prefix(APP_ACTION_PREFIX) else {
                continue;
            };
            button_actions.insert(
                button.action.clone(),
                GtkAction {
                    name: name.to_owned(),
                    target: button.target,
                },
            );
            actions_flat.push(button.action);
            actions_flat.push(button.label);
        }

        let default_action = owned_string(map.get("default-action")).and_then(|action| {
            action.strip_prefix(APP_ACTION_PREFIX).map(|name| GtkAction {
                name: name.to_owned(),
                target: map.get("default-action-target").and_then(try_clone_owned),
            })
        });

        let hints = build_hints(urgency, image_path.as_deref(), app_id);

        Self {
            title,
            body,
            app_icon,
            hints,
            actions_flat,
            dispatch: GtkDispatch {
                app_id: app_id.to_owned(),
                gtk_id: gtk_id.to_owned(),
                default_action,
                button_actions,
            },
        }
    }
}

/// Builds the synthetic hints map so the shared `from_props` derives urgency, the icon
/// file path, and the desktop entry (used by the shell for icon resolution).
fn build_hints(urgency: Urgency, image_path: Option<&str>, app_id: &str) -> NotificationHints {
    let mut hints = NotificationHints::new();
    if let Ok(value) = OwnedValue::try_from(Value::U8(urgency as u8)) {
        hints.insert(String::from("urgency"), value);
    }
    hints.insert(
        String::from("desktop-entry"),
        OwnedValue::from(Str::from(app_id)),
    );
    if let Some(path) = image_path {
        hints.insert(
            String::from("image-path"),
            OwnedValue::from(Str::from(path)),
        );
    }
    hints
}

struct ParsedButton {
    label: String,
    action: String,
    target: Option<OwnedValue>,
}

fn parse_buttons(value: Option<&OwnedValue>) -> Vec<ParsedButton> {
    let mut buttons = Vec::new();
    let Some(value) = value else {
        return buttons;
    };
    let Value::Array(array) = &**value else {
        return buttons;
    };

    for element in array.iter() {
        let Ok(cloned) = element.try_clone() else {
            continue;
        };
        let Ok(row) = HashMap::<String, OwnedValue>::try_from(cloned) else {
            continue;
        };
        let (Some(label), Some(action)) =
            (owned_string(row.get("label")), owned_string(row.get("action")))
        else {
            continue;
        };
        buttons.push(ParsedButton {
            label,
            action,
            target: row.get("target").and_then(try_clone_owned),
        });
    }

    buttons
}

/// Parses a serialized `GIcon` `(sv)` into `(app_icon, image_path)`: a themed icon → its
/// first name (`app_icon`); a file icon → its path (`image_path`); a bytes icon → a
/// cached file (`image_path`). Unrecognized shapes yield `(None, None)` — the shell then
/// falls back to the `desktop-entry` icon.
fn parse_icon(value: &OwnedValue) -> (Option<String>, Option<String>) {
    let Value::Structure(structure) = &**value else {
        return (None, None);
    };
    let [Value::Str(tag), Value::Value(payload)] = structure.fields() else {
        return (None, None);
    };

    match tag.as_str() {
        "themed" => {
            if let Value::Array(names) = &**payload
                && let Some(Value::Str(name)) = names.iter().next()
            {
                return (Some(name.to_string()), None);
            }
            (None, None)
        }
        "file" => {
            if let Value::Str(path) = &**payload {
                return (None, Some(strip_file_uri(path.as_str())));
            }
            (None, None)
        }
        "bytes" => {
            if let Value::Array(array) = &**payload {
                let bytes: Vec<u8> = array
                    .iter()
                    .filter_map(|value| match value {
                        Value::U8(byte) => Some(*byte),
                        _ => None,
                    })
                    .collect();
                return (None, image_cache::cache_encoded_image(&bytes));
            }
            (None, None)
        }
        _ => (None, None),
    }
}

fn strip_file_uri(path: &str) -> String {
    path.strip_prefix("file://").unwrap_or(path).to_owned()
}

fn owned_string(value: Option<&OwnedValue>) -> Option<String> {
    value.and_then(|value| value.downcast_ref::<String>().ok())
}

fn try_clone_owned(value: &OwnedValue) -> Option<OwnedValue> {
    value.try_clone().ok()
}
