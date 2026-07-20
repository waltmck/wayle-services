use std::{
    collections::HashMap,
    env, fs,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, Utc};
use derive_more::Debug;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument, warn};
use zbus::zvariant::{
    LE, OwnedValue, Str, Value,
    serialized::{Context, Data},
    to_bytes,
};

use crate::{
    core::{
        notification::Notification,
        types::{Action, GtkAction, GtkDispatch, IMAGE_DATA_KEYS, NotificationSource},
    },
    error::Error,
};

#[derive(Debug)]
pub(crate) struct StoredNotification {
    pub id: u32,
    pub app_name: Option<String>,
    pub replaces_id: Option<u32>,
    pub app_icon: Option<String>,
    pub summary: String,
    pub body: Option<String>,
    pub actions: Vec<String>,
    pub hints: HashMap<String, OwnedValue>,
    pub image_path: Option<String>,
    pub expire_timeout: Option<u32>,
    pub timestamp: i64,
    pub owner: Option<String>,
    pub source: NotificationSource,
}

impl From<&Notification> for StoredNotification {
    fn from(notification: &Notification) -> Self {
        Self {
            id: notification.id,
            app_name: notification.app_name.get().clone(),
            replaces_id: notification.replaces_id.get(),
            app_icon: notification.app_icon.get().clone(),
            summary: notification.summary.get().clone(),
            body: notification.body.get().clone(),
            actions: Action::to_dbus_format(&notification.actions.get()),
            hints: notification.hints.get().clone().unwrap_or_default(),
            image_path: notification.image_path.get().clone(),
            expire_timeout: notification.expire_timeout.get(),
            timestamp: notification.timestamp.get().timestamp_millis(),
            owner: notification.owner.get(),
            source: notification.source.get(),
        }
    }
}

/// On-disk form of [`NotificationSource`]. Kept separate from the runtime type so action
/// targets serialize through the same `serde_json::Value` round-trip proven to work for
/// `OwnedValue` hints.
#[derive(Serialize, Deserialize)]
struct StoredSource {
    kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    gtk: Option<StoredGtkDispatch>,
}

#[derive(Serialize, Deserialize)]
struct StoredGtkDispatch {
    app_id: String,
    gtk_id: String,
    default_action: Option<StoredGtkAction>,
    button_actions: HashMap<String, StoredGtkAction>,
}

#[derive(Serialize, Deserialize)]
struct StoredGtkAction {
    name: String,
    /// The action target, stored as its D-Bus wire encoding (a self-describing variant)
    /// held as a JSON byte array. See [`encode_target`] for why the encoding is bytes and
    /// not `serde_json` of the `OwnedValue` directly.
    target: Option<String>,
}

fn serialize_source(source: &NotificationSource) -> String {
    let stored = match source {
        NotificationSource::Freedesktop => StoredSource {
            kind: String::from("fdo"),
            gtk: None,
        },
        NotificationSource::Gtk(dispatch) => StoredSource {
            kind: String::from("gtk"),
            gtk: Some(StoredGtkDispatch {
                app_id: dispatch.app_id.clone(),
                gtk_id: dispatch.gtk_id.clone(),
                default_action: dispatch.default_action.as_ref().map(stored_action),
                button_actions: dispatch
                    .button_actions
                    .iter()
                    .map(|(key, action)| (key.clone(), stored_action(action)))
                    .collect(),
            }),
        },
    };
    serde_json::to_string(&stored).unwrap_or_else(|err| {
        warn!(error = %err, "cannot serialize notification source");
        String::from(r#"{"kind":"fdo"}"#)
    })
}

fn deserialize_source(raw: Option<String>) -> NotificationSource {
    let Some(raw) = raw else {
        return NotificationSource::Freedesktop;
    };
    let Ok(stored) = serde_json::from_str::<StoredSource>(&raw) else {
        warn!("cannot deserialize notification source; treating as freedesktop");
        return NotificationSource::Freedesktop;
    };
    match (stored.kind.as_str(), stored.gtk) {
        ("gtk", Some(gtk)) => NotificationSource::Gtk(GtkDispatch {
            app_id: gtk.app_id,
            gtk_id: gtk.gtk_id,
            default_action: gtk.default_action.map(gtk_action_from_stored),
            button_actions: gtk
                .button_actions
                .into_iter()
                .map(|(key, action)| (key, gtk_action_from_stored(action)))
                .collect(),
        }),
        _ => NotificationSource::Freedesktop,
    }
}

fn stored_action(action: &GtkAction) -> StoredGtkAction {
    StoredGtkAction {
        name: action.name.clone(),
        target: action.target.as_ref().and_then(encode_target),
    }
}

fn gtk_action_from_stored(stored: StoredGtkAction) -> GtkAction {
    GtkAction {
        name: stored.name,
        target: stored.target.as_deref().and_then(decode_target),
    }
}

/// Serializes an action target to the D-Bus wire format (a self-describing variant that
/// embeds its own signature) held as a JSON byte array.
///
/// The obvious `serde_json::to_string(&owned_value)` is *not* reversible: `OwnedValue`'s
/// deserializer rebuilds borrowed scalars/strings from JSON but cannot reconstruct a
/// `Structure` or other composite variant — it needs the D-Bus type system, not JSON's.
/// So a structured target such as a `(yay)` serialized fine yet came back as `None` on
/// reload, and the action was then dispatched with an empty parameter, which the app
/// rejects ("expected type (yay) but got type ()"). The wire encoding carries the full
/// signature, so any GVariant an app attaches survives store→load intact.
fn encode_target(target: &OwnedValue) -> Option<String> {
    let value: &Value = target;
    let data = to_bytes(Context::new_dbus(LE, 0), value).ok()?;
    serde_json::to_string(&*data).ok()
}

/// Inverse of [`encode_target`]. Falls back to the legacy `serde_json`-of-`OwnedValue`
/// encoding so targets persisted before the wire-format switch (scalars/strings, which
/// that path could still read) keep working across the upgrade.
fn decode_target(stored: &str) -> Option<OwnedValue> {
    if let Ok(bytes) = serde_json::from_str::<Vec<u8>>(stored) {
        let data = Data::new(bytes, Context::new_dbus(LE, 0));
        if let Ok((value, _)) = data.deserialize::<Value<'_>>() {
            return OwnedValue::try_from(value).ok();
        }
    }
    serde_json::from_str::<OwnedValue>(stored).ok()
}

#[derive(Debug, Clone)]
pub(crate) struct NotificationStore {
    #[debug(skip)]
    connection: Arc<Mutex<Connection>>,
}

impl NotificationStore {
    #[instrument(err)]
    pub fn new() -> Result<Self, Error> {
        let home = env::var("HOME")
            .map_err(|_| Error::DatabaseError(String::from("HOME environment variable not set")))?;

        let data_dir = format!("{home}/.local/share/wayle");
        fs::create_dir_all(&data_dir)
            .map_err(|err| Error::DatabaseError(format!("cannot create data directory: {err}")))?;

        let db_path = format!("{data_dir}/notifications.db");
        debug!(path = %db_path, "notification store opened");
        let connection = Connection::open(db_path)
            .map_err(|err| Error::DatabaseError(format!("cannot open database: {err}")))?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS notifications (
                    id INTEGER PRIMARY KEY,
                    app_name TEXT,
                    replaces_id INTEGER,
                    app_icon TEXT,
                    summary TEXT NOT NULL,
                    body TEXT,
                    actions TEXT NOT NULL,
                    hints TEXT NOT NULL,
                    expire_timeout INTEGER,
                    timestamp INTEGER NOT NULL,
                    image_path TEXT,
                    owner TEXT,
                    source TEXT
                )",
                [],
            )
            .map_err(|err| Error::DatabaseError(format!("cannot create table: {err}")))?;

        // Migrate pre-existing databases that lack the `owner` column (CREATE TABLE IF
        // NOT EXISTS won't add it, and SQLite has no ADD COLUMN IF NOT EXISTS).
        let has_owner_column = {
            let mut stmt = connection
                .prepare("PRAGMA table_info(notifications)")
                .map_err(|err| Error::DatabaseError(format!("cannot inspect schema: {err}")))?;
            let columns = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .map_err(|err| Error::DatabaseError(format!("cannot read schema: {err}")))?;
            columns
                .filter_map(Result::ok)
                .any(|column| column == "owner")
        };
        if !has_owner_column {
            connection
                .execute("ALTER TABLE notifications ADD COLUMN owner TEXT", [])
                .map_err(|err| Error::DatabaseError(format!("cannot add owner column: {err}")))?;
        }

        // Likewise migrate databases that predate the `source` column (freedesktop vs GTK
        // origin + GTK action-dispatch data). Rows without it read back as freedesktop.
        let has_source_column = {
            let mut stmt = connection
                .prepare("PRAGMA table_info(notifications)")
                .map_err(|err| Error::DatabaseError(format!("cannot inspect schema: {err}")))?;
            let columns = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .map_err(|err| Error::DatabaseError(format!("cannot read schema: {err}")))?;
            columns
                .filter_map(Result::ok)
                .any(|column| column == "source")
        };
        if !has_source_column {
            connection
                .execute("ALTER TABLE notifications ADD COLUMN source TEXT", [])
                .map_err(|err| Error::DatabaseError(format!("cannot add source column: {err}")))?;
        }

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                )",
                [],
            )
            .map_err(|err| Error::DatabaseError(format!("cannot create metadata table: {err}")))?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS metadata_text (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )",
                [],
            )
            .map_err(|err| {
                Error::DatabaseError(format!("cannot create metadata_text table: {err}"))
            })?;

        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;",
            )
            .map_err(|err| Error::DatabaseError(format!("cannot set pragmas: {err}")))?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Highest notification id ever issued.
    ///
    /// Persisted so the id counter can resume above it after a restart instead of
    /// rewinding to the max surviving notification — a rewind reuses ids that
    /// long-lived clients still hold, so an action invoked on a new notification
    /// would reach the wrong, stale client.
    #[instrument(skip(self), err)]
    pub fn id_high_water(&self) -> Result<u32, Error> {
        let conn = self
            .connection
            .lock()
            .map_err(|_| Error::DatabaseError("cannot acquire lock on database".to_string()))?;
        let value: Option<u32> = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = 'id_high_water'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| Error::DatabaseError(format!("cannot read id high-water: {err}")))?;
        Ok(value.unwrap_or(0))
    }

    /// Records `id` as issued, advancing the persisted high-water mark if higher.
    #[instrument(skip(self), err)]
    pub fn record_id_high_water(&self, id: u32) -> Result<(), Error> {
        self.connection
            .lock()
            .map_err(|_| Error::DatabaseError("cannot acquire lock on database".to_string()))?
            .execute(
                "INSERT INTO metadata (key, value) VALUES ('id_high_water', ?1)
                 ON CONFLICT(key) DO UPDATE SET value = MAX(value, excluded.value)",
                params![id],
            )
            .map_err(|err| Error::DatabaseError(format!("cannot record id high-water: {err}")))?;
        Ok(())
    }

    /// The session bus GUID recorded when notifications were last persisted, if any.
    ///
    /// Owner unique names are only meaningful within one session bus lifetime, so a
    /// mismatch means the persisted owners are stale and must not be directed to.
    #[instrument(skip(self), err)]
    pub fn bus_guid(&self) -> Result<Option<String>, Error> {
        let conn = self
            .connection
            .lock()
            .map_err(|_| Error::DatabaseError("cannot acquire lock on database".to_string()))?;
        let value: Option<String> = conn
            .query_row(
                "SELECT value FROM metadata_text WHERE key = 'bus_guid'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| Error::DatabaseError(format!("cannot read bus guid: {err}")))?;
        Ok(value)
    }

    /// Records the current session bus GUID.
    #[instrument(skip(self), err)]
    pub fn record_bus_guid(&self, guid: &str) -> Result<(), Error> {
        self.connection
            .lock()
            .map_err(|_| Error::DatabaseError("cannot acquire lock on database".to_string()))?
            .execute(
                "INSERT INTO metadata_text (key, value) VALUES ('bus_guid', ?1)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![guid],
            )
            .map_err(|err| Error::DatabaseError(format!("cannot record bus guid: {err}")))?;
        Ok(())
    }

    #[instrument(skip(self, notification), fields(id = notification.id, summary = %notification.summary.get()), err)]
    pub fn add(&self, notification: &Notification) -> Result<(), Error> {
        let stored = StoredNotification::from(notification);

        let actions_json = serde_json::to_string(&stored.actions)
            .map_err(|err| Error::DatabaseError(format!("cannot serialize actions: {err}")))?;

        let mut hints_for_storage = stored.hints.clone();
        for key in &IMAGE_DATA_KEYS {
            hints_for_storage.remove(*key);
        }
        let hints_json = serde_json::to_string(&hints_for_storage)
            .map_err(|err| Error::DatabaseError(format!("cannot serialize hints: {err}")))?;

        let source_json = serialize_source(&stored.source);

        self.connection
            .lock()
            .map_err(|_| Error::DatabaseError("cannot acquire lock on database".to_string()))?
            .execute(
                "INSERT OR REPLACE INTO notifications
                 (id, app_name, replaces_id, app_icon, summary, body, actions, hints,
                 expire_timeout, timestamp, image_path, owner, source)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    stored.id,
                    stored.app_name,
                    stored.replaces_id,
                    stored.app_icon,
                    stored.summary,
                    stored.body,
                    actions_json,
                    hints_json,
                    stored.expire_timeout,
                    stored.timestamp,
                    stored.image_path,
                    stored.owner,
                    source_json,
                ],
            )
            .map_err(|err| Error::DatabaseError(format!("cannot store notification: {err}")))?;

        Ok(())
    }

    #[instrument(skip(self), fields(notification_id = id), err)]
    pub fn remove(&self, id: u32) -> Result<(), Error> {
        self.connection
            .lock()
            .map_err(|_| Error::DatabaseError("cannot acquire lock on database".to_string()))?
            .execute("DELETE FROM notifications WHERE id = ?1", params![id])
            .map_err(|err| Error::DatabaseError(format!("cannot remove notification: {err}")))?;

        Ok(())
    }

    /// Removes several notifications in a single atomic `DELETE` rather than one statement
    /// per id. The ids are `u32`, so inlining them in the `IN` clause is injection-safe.
    #[instrument(skip(self, ids), fields(count = ids.len()), err)]
    pub fn remove_many(&self, ids: &[u32]) -> Result<(), Error> {
        if ids.is_empty() {
            return Ok(());
        }

        let placeholders = ids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",");

        self.connection
            .lock()
            .map_err(|_| Error::DatabaseError("cannot acquire lock on database".to_string()))?
            .execute(
                &format!("DELETE FROM notifications WHERE id IN ({placeholders})"),
                [],
            )
            .map_err(|err| Error::DatabaseError(format!("cannot remove notifications: {err}")))?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    pub fn load_all(&self, remove_expired: bool) -> Result<Vec<StoredNotification>, Error> {
        let conn = self
            .connection
            .lock()
            .map_err(|_| Error::DatabaseError("cannot acquire lock on database".to_string()))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, app_name, replaces_id, app_icon, summary, body,
                 actions, hints, expire_timeout, timestamp, image_path, owner, source
                 FROM notifications
                 ORDER BY timestamp DESC",
            )
            .map_err(|err| Error::DatabaseError(format!("cannot prepare query: {err}")))?;

        let notifications = stmt
            .query_map([], |row| {
                let actions_json: String = row.get(6)?;
                let hints_json: String = row.get(7)?;
                let image_path: Option<String> = row.get(10)?;
                let source: Option<String> = row.get(12)?;

                let actions: Vec<String> =
                    serde_json::from_str(&actions_json).unwrap_or_else(|err| {
                        warn!(error = %err, "cannot deserialize actions");
                        Vec::new()
                    });
                let hints_json_map: HashMap<String, serde_json::Value> =
                    serde_json::from_str(&hints_json).unwrap_or_else(|err| {
                        warn!(error = %err, "cannot deserialize hints");
                        HashMap::new()
                    });
                let mut hints: HashMap<String, OwnedValue> = hints_json_map
                    .into_iter()
                    .filter_map(|(key, value)| {
                        // `from_value` cannot rebuild string-valued `OwnedValue`s — its
                        // deserializer needs *borrowed* input — so it silently dropped
                        // every string hint on reload (desktop-entry, category,
                        // sound-file, ...). Re-serialize and go through `from_str`, which
                        // borrows from the JSON text and round-trips correctly.
                        let json = serde_json::to_string(&value).ok()?;
                        serde_json::from_str::<OwnedValue>(&json)
                            .ok()
                            .map(|owned_value| (key, owned_value))
                    })
                    .collect();

                if let Some(ref path) = image_path {
                    hints.insert(
                        String::from("image-path"),
                        OwnedValue::from(Str::from(path.as_str())),
                    );
                }

                Ok(StoredNotification {
                    id: row.get(0)?,
                    app_name: row.get(1)?,
                    replaces_id: row.get(2)?,
                    app_icon: row.get(3)?,
                    summary: row.get(4)?,
                    body: row.get(5)?,
                    actions,
                    hints,
                    image_path,
                    expire_timeout: row.get(8)?,
                    timestamp: row.get(9)?,
                    owner: row.get(11)?,
                    source: deserialize_source(source),
                })
            })
            .map_err(|err| Error::DatabaseError(format!("cannot query notifications: {err}")))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| Error::DatabaseError(format!("cannot parse notifications: {err}")))?;

        if !remove_expired {
            debug!(count = notifications.len(), "loaded stored notifications");
            return Ok(notifications);
        }

        let now = Utc::now();
        let notifications: Vec<StoredNotification> = notifications
            .into_iter()
            .filter(|notif| {
                let Some(timeout) = notif.expire_timeout else {
                    return true;
                };
                let Some(timestamp) = DateTime::<Utc>::from_timestamp_millis(notif.timestamp)
                else {
                    return false;
                };
                timestamp + Duration::from_millis(timeout as u64) > now
            })
            .collect();

        debug!(
            count = notifications.len(),
            "loaded stored notifications (expired filtered)"
        );
        Ok(notifications)
    }
}

#[cfg(test)]
mod tests {
    use zbus::zvariant::{ObjectPath, OwnedValue, Str, Value};

    use super::*;

    fn owned(value: Value<'_>) -> OwnedValue {
        OwnedValue::try_from(value).expect("value converts to OwnedValue")
    }

    #[test]
    fn source_round_trips_gtk_dispatch_with_target() {
        let mut button_actions = HashMap::new();
        button_actions.insert(
            String::from("app.open"),
            GtkAction {
                name: String::from("open"),
                target: Some(OwnedValue::from(Str::from("chat-42"))),
            },
        );
        let source = NotificationSource::Gtk(GtkDispatch {
            app_id: String::from("org.gnome.Fractal"),
            gtk_id: String::from("msg-1"),
            default_action: Some(GtkAction {
                name: String::from("show"),
                target: None,
            }),
            button_actions,
        });

        let restored = deserialize_source(Some(serialize_source(&source)));

        let NotificationSource::Gtk(dispatch) = restored else {
            panic!("expected a gtk source");
        };
        assert_eq!(dispatch.app_id, "org.gnome.Fractal");
        assert_eq!(dispatch.gtk_id, "msg-1");
        let default = dispatch.default_action.expect("default action preserved");
        assert_eq!(default.name, "show");
        assert!(default.target.is_none());
        let action = dispatch
            .button_actions
            .get("app.open")
            .expect("button action preserved");
        assert_eq!(action.name, "open");
        let target = action.target.as_ref().expect("target preserved");
        assert_eq!(target.downcast_ref::<String>().unwrap(), "chat-42");
    }

    /// Regression guard for a *structured* action target (e.g. a `(yay)`): it must
    /// survive store→load. `serde_json::from_str::<OwnedValue>` rebuilds borrowed
    /// scalars/strings from JSON but cannot reconstruct a `Structure`/composite variant,
    /// so the previous `serde_json`-of-`OwnedValue` encoding serialized such a target
    /// fine yet deserialized it back to `None`. The action was then dispatched with an
    /// empty parameter and the app rejected it ("expected type (yay) but got type ()").
    #[test]
    fn source_round_trips_structured_default_action_target() {
        let target = OwnedValue::try_from(Value::new((5u8, vec![1u8, 2, 3]))).unwrap();
        assert_eq!(target.value_signature().to_string(), "(yay)");

        let source = NotificationSource::Gtk(GtkDispatch {
            app_id: String::from("de.schmidhuberj.Flare"),
            gtk_id: String::from("msg-1"),
            default_action: Some(GtkAction {
                name: String::from("notification-clicked"),
                target: Some(target.try_clone().unwrap()),
            }),
            button_actions: HashMap::new(),
        });

        let restored = deserialize_source(Some(serialize_source(&source)));

        let NotificationSource::Gtk(dispatch) = restored else {
            panic!("expected a gtk source");
        };
        let default = dispatch.default_action.expect("default action preserved");
        assert_eq!(default.name, "notification-clicked");
        let restored_target = default.target.expect("structured target preserved");
        assert_eq!(restored_target.value_signature().to_string(), "(yay)");
        assert_eq!(restored_target, target);
    }

    /// The target encoding must survive store→load for every shape of GVariant an app
    /// might attach to an action — scalars, strings, object paths, arrays, tuples,
    /// nested tuples and dicts. Each case asserts (a) the signature is unchanged and
    /// (b) re-encoding the restored value yields byte-identical wire data, which proves
    /// value *and* type survived exactly without relying on `OwnedValue`'s equality for
    /// composite types.
    #[test]
    fn action_target_round_trips_across_gvariant_schemas() {
        let mut dict_ss: HashMap<String, String> = HashMap::new();
        dict_ss.insert(String::from("key"), String::from("value"));
        let mut dict_si: HashMap<String, i32> = HashMap::new();
        dict_si.insert(String::from("count"), 7);
        let mut dict_sv: HashMap<String, Value> = HashMap::new();
        dict_sv.insert(String::from("id"), Value::new(42i32));

        let cases: Vec<(&str, OwnedValue)> = vec![
            // Scalars, one per D-Bus basic type.
            ("y", owned(Value::U8(255))),
            ("b", owned(Value::Bool(true))),
            ("n", owned(Value::I16(-1234))),
            ("q", owned(Value::U16(60000))),
            ("i", owned(Value::I32(-42))),
            ("u", owned(Value::U32(42))),
            ("x", owned(Value::I64(-5_000_000_000))),
            ("t", owned(Value::U64(5_000_000_000))),
            ("d", owned(Value::F64(3.5))),
            ("s", owned(Value::new("conversation-42"))),
            (
                "o",
                owned(Value::new(
                    ObjectPath::try_from("/de/schmidhuberj/Flare/chat/7").unwrap(),
                )),
            ),
            // Arrays, including the byte array a naive JSON encoding could mangle.
            ("ay", owned(Value::new(vec![0u8, 1, 2, 255]))),
            ("ai", owned(Value::new(vec![1i32, -2, 3]))),
            (
                "as",
                owned(Value::new(vec![String::from("a"), String::from("b")])),
            ),
            // Tuples / structures, the shapes the old serde_json path silently dropped.
            ("(yay)", owned(Value::new((5u8, vec![1u8, 2, 3])))),
            ("(si)", owned(Value::new((String::from("chat"), 7i32)))),
            (
                "(ss)",
                owned(Value::new((String::from("a"), String::from("b")))),
            ),
            (
                "((si)s)",
                owned(Value::new((
                    (String::from("inner"), 7i32),
                    String::from("outer"),
                ))),
            ),
            // Dicts, including a vardict (a{sv}) whose values are themselves variants.
            ("a{ss}", owned(Value::new(dict_ss))),
            ("a{si}", owned(Value::new(dict_si))),
            ("a{sv}", owned(Value::new(dict_sv))),
        ];

        for (expected_sig, target) in &cases {
            let encoded =
                encode_target(target).unwrap_or_else(|| panic!("encode failed for {expected_sig}"));
            let decoded = decode_target(&encoded)
                .unwrap_or_else(|| panic!("decode failed for {expected_sig}"));
            assert_eq!(
                decoded.value_signature().to_string(),
                *expected_sig,
                "signature changed for {expected_sig}"
            );
            let reencoded = encode_target(&decoded)
                .unwrap_or_else(|| panic!("re-encode failed for {expected_sig}"));
            assert_eq!(reencoded, encoded, "wire bytes changed for {expected_sig}");
        }
    }

    /// End-to-end through `serialize_source`/`deserialize_source`: a dispatch with a
    /// structured default action plus several buttons carrying differently-typed targets
    /// (and one with none) must come back intact, keyed correctly.
    #[test]
    fn source_round_trips_dispatch_with_mixed_target_schemas() {
        let mut button_actions = HashMap::new();
        button_actions.insert(
            String::from("app.reply"),
            GtkAction {
                name: String::from("reply"),
                target: Some(owned(Value::new("thread-9"))),
            },
        );
        button_actions.insert(
            String::from("app.mark"),
            GtkAction {
                name: String::from("mark"),
                target: Some(owned(Value::new((1u8, vec![9u8, 8, 7])))),
            },
        );
        button_actions.insert(
            String::from("app.count"),
            GtkAction {
                name: String::from("count"),
                target: Some(owned(Value::U32(3))),
            },
        );
        button_actions.insert(
            String::from("app.dismiss"),
            GtkAction {
                name: String::from("dismiss"),
                target: None,
            },
        );

        let source = NotificationSource::Gtk(GtkDispatch {
            app_id: String::from("de.schmidhuberj.Flare"),
            gtk_id: String::from("msg-7"),
            default_action: Some(GtkAction {
                name: String::from("notification-clicked"),
                target: Some(owned(Value::new((5u8, vec![1u8, 2, 3])))),
            }),
            button_actions,
        });

        let NotificationSource::Gtk(dispatch) = deserialize_source(Some(serialize_source(&source)))
        else {
            panic!("expected a gtk source");
        };

        let default = dispatch.default_action.expect("default action preserved");
        assert_eq!(default.name, "notification-clicked");
        assert_eq!(
            default.target.unwrap().value_signature().to_string(),
            "(yay)"
        );

        let reply = dispatch
            .button_actions
            .get("app.reply")
            .expect("reply button preserved");
        assert_eq!(reply.name, "reply");
        assert_eq!(
            reply
                .target
                .as_ref()
                .unwrap()
                .downcast_ref::<String>()
                .unwrap(),
            "thread-9"
        );

        let mark = dispatch
            .button_actions
            .get("app.mark")
            .expect("mark button preserved");
        assert_eq!(
            mark.target.as_ref().unwrap().value_signature().to_string(),
            "(yay)"
        );

        let count = dispatch
            .button_actions
            .get("app.count")
            .expect("count button preserved");
        assert_eq!(
            count.target.as_ref().unwrap().value_signature().to_string(),
            "u"
        );

        let dismiss = dispatch
            .button_actions
            .get("app.dismiss")
            .expect("dismiss button preserved");
        assert!(dismiss.target.is_none());
    }

    /// Backward compatibility: targets persisted before the wire-format switch were
    /// stored as `serde_json::to_string(&OwnedValue)`. `decode_target` must still read
    /// those (for the scalar/string values that legacy path could round-trip) so an
    /// upgrade doesn't silently drop targets from already-stored notifications.
    #[test]
    fn decode_target_reads_legacy_json_string_encoding() {
        let legacy = serde_json::to_string(&OwnedValue::from(Str::from("chat-42")))
            .expect("legacy encode succeeds");
        // Sanity: the legacy form is the old object encoding, not the new byte array.
        assert!(serde_json::from_str::<Vec<u8>>(&legacy).is_err());

        let decoded = decode_target(&legacy).expect("legacy target still decodes");
        assert_eq!(decoded.downcast_ref::<String>().unwrap(), "chat-42");
    }

    /// A `None` target must serialize away and come back `None` (a body click with no
    /// default-action target dispatches a plain `Activate`, not `ActivateAction([])`).
    #[test]
    fn none_target_round_trips_as_none() {
        let stored = stored_action(&GtkAction {
            name: String::from("show"),
            target: None,
        });
        assert!(stored.target.is_none());
        let restored = gtk_action_from_stored(stored);
        assert_eq!(restored.name, "show");
        assert!(restored.target.is_none());
    }

    /// Serialize hints as `add()` does, then rebuild them as `load_all` does.
    fn roundtrip(hints: &HashMap<String, OwnedValue>) -> HashMap<String, OwnedValue> {
        let json = serde_json::to_string(hints).expect("serialize hints");
        let value_map: HashMap<String, serde_json::Value> =
            serde_json::from_str(&json).expect("parse hints json");
        value_map
            .into_iter()
            .filter_map(|(key, value)| {
                let json = serde_json::to_string(&value).ok()?;
                serde_json::from_str::<OwnedValue>(&json)
                    .ok()
                    .map(|owned| (key, owned))
            })
            .collect()
    }

    /// Regression guard: string-valued hints must survive the store→load JSON
    /// round-trip. `serde_json::from_value::<OwnedValue>` used to drop them
    /// (needs borrowed input), which silently lost `desktop-entry` on reload.
    #[test]
    fn string_hint_survives_roundtrip() {
        let mut hints: HashMap<String, OwnedValue> = HashMap::new();
        hints.insert(
            String::from("desktop-entry"),
            OwnedValue::from(Str::from("org.gnome.clocks")),
        );

        let loaded = roundtrip(&hints);

        let desktop_entry = loaded
            .get("desktop-entry")
            .and_then(|hint| hint.downcast_ref::<String>().ok());
        assert_eq!(desktop_entry.as_deref(), Some("org.gnome.clocks"));
    }

    #[test]
    fn source_absent_or_unknown_defaults_to_freedesktop() {
        assert!(matches!(
            deserialize_source(None),
            NotificationSource::Freedesktop
        ));
        assert!(matches!(
            deserialize_source(Some(String::from("garbage"))),
            NotificationSource::Freedesktop
        ));
    }
}
