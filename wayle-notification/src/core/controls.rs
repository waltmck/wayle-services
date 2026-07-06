use tracing::instrument;
use zbus::Connection;

use crate::{
    error::Error,
    types::{
        Signal,
        dbus::{SERVICE_INTERFACE, SERVICE_PATH},
    },
};

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
}
