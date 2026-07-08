use std::collections::HashMap;

use tracing::instrument;
use zbus::{Connection, zvariant::OwnedValue};

use crate::{
    core::types::{GtkAction, gtk_object_path},
    error::Error,
    types::{
        Signal,
        dbus::{SERVICE_INTERFACE, SERVICE_PATH},
    },
};

const APPLICATION_INTERFACE: &str = "org.freedesktop.Application";

pub(super) struct NotificationControls;

impl NotificationControls {
    #[instrument(skip(connection), fields(notification_id = %id, action = %action_key), err)]
    pub(super) async fn invoke(
        connection: &Connection,
        id: &u32,
        action_key: &str,
        owner: Option<&str>,
    ) -> Result<(), Error> {
        // Direct the signal to the notification's owning connection. If the owner is
        // unknown, skip emission rather than broadcasting: a broadcast lets clients
        // that don't filter by id react to notifications they didn't create.
        let Some(owner) = owner else {
            return Ok(());
        };

        connection
            .emit_signal(
                Some(owner),
                SERVICE_PATH,
                SERVICE_INTERFACE,
                Signal::ActionInvoked.as_str(),
                &(id, action_key),
            )
            .await?;

        Ok(())
    }

    /// Dispatches a GTK notification action via `org.freedesktop.Application`.
    ///
    /// The D-Bus daemon cold-launches the app through service activation if it is not
    /// running (works for `DBusActivatable` apps). `action = Some` →
    /// `ActivateAction(name, [target?], {})`; `action = None` (a body click on a
    /// notification with no default action) → `Activate({})` to raise/launch the app.
    #[instrument(skip(connection, action), fields(app_id = %app_id), err)]
    pub(super) async fn activate_gtk(
        connection: &Connection,
        app_id: &str,
        action: Option<&GtkAction>,
    ) -> Result<(), Error> {
        let object_path = gtk_object_path(app_id);
        let platform_data: HashMap<String, OwnedValue> = HashMap::new();

        match action {
            Some(action) => {
                // `av` parameter: zero or one target variant, matching GNOME's shell.
                let parameter: Vec<OwnedValue> = action
                    .target
                    .as_ref()
                    .and_then(|target| target.try_clone().ok())
                    .into_iter()
                    .collect();
                connection
                    .call_method(
                        Some(app_id),
                        object_path.as_str(),
                        Some(APPLICATION_INTERFACE),
                        "ActivateAction",
                        &(action.name.as_str(), parameter, platform_data),
                    )
                    .await?;
            }
            None => {
                connection
                    .call_method(
                        Some(app_id),
                        object_path.as_str(),
                        Some(APPLICATION_INTERFACE),
                        "Activate",
                        &(platform_data,),
                    )
                    .await?;
            }
        }

        Ok(())
    }
}
