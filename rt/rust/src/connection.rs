use std::{
    cell::RefCell, collections::HashMap, marker::PhantomData, ops::Deref, path::Path, sync::mpsc,
};

use prost::Message;

use crate::{Change, Result, Row};

#[derive(Clone)]
struct RawChange {
    id: String,
    at_ns: i64,
    data: Option<Vec<u8>>,
}

#[derive(Default)]
struct Broker {
    listeners: HashMap<&'static str, Vec<mpsc::Sender<RawChange>>>,
    pending: Vec<(&'static str, RawChange)>,
}

pub struct Connection {
    sqlite: rusqlite::Connection,
    broker: RefCell<Broker>,
}

impl Connection {
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Ok(Self::from(rusqlite::Connection::open(path)?))
    }

    pub fn open_in_memory() -> rusqlite::Result<Self> {
        Ok(Self::from(rusqlite::Connection::open_in_memory()?))
    }

    pub fn transaction(&self) -> Result<Transaction<'_>> {
        self.sqlite.execute_batch("BEGIN IMMEDIATE")?;
        Ok(Transaction {
            connection: self,
            finished: false,
        })
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.broker.borrow().pending.len()
    }

    pub(crate) fn truncate_changes(&self, count: usize) {
        self.broker.borrow_mut().pending.truncate(count);
    }

    pub(crate) fn flush_changes(&self) {
        if !self.sqlite.is_autocommit() {
            return;
        }
        let mut broker = self.broker.borrow_mut();
        let pending = std::mem::take(&mut broker.pending);
        for (table, change) in pending {
            if let Some(listeners) = broker.listeners.get_mut(table) {
                listeners.retain(|sender| sender.send(change.clone()).is_ok());
            }
        }
    }

    pub(crate) fn notify<T: Message>(&self, table: &'static str, change: Change<T>) {
        let change = match change {
            Change::Upsert(row) => RawChange {
                id: row.id,
                at_ns: row.at_ns,
                data: Some(row.data.encode_to_vec()),
            },
            Change::Delete { id, at_ns } => RawChange {
                id,
                at_ns,
                data: None,
            },
        };
        self.broker.borrow_mut().pending.push((table, change));
        self.flush_changes();
    }

    pub(crate) fn listen<T: Message + Default>(&self, table: &'static str) -> ChangeReceiver<T> {
        let (sender, receiver) = mpsc::channel();
        self.broker
            .borrow_mut()
            .listeners
            .entry(table)
            .or_default()
            .push(sender);
        ChangeReceiver {
            receiver,
            data: PhantomData,
        }
    }
}

impl From<rusqlite::Connection> for Connection {
    fn from(sqlite: rusqlite::Connection) -> Self {
        Self {
            sqlite,
            broker: RefCell::new(Broker::default()),
        }
    }
}

impl Deref for Connection {
    type Target = rusqlite::Connection;
    fn deref(&self) -> &Self::Target {
        &self.sqlite
    }
}

pub struct Transaction<'a> {
    connection: &'a Connection,
    finished: bool,
}

impl Transaction<'_> {
    pub fn commit(mut self) -> Result<()> {
        self.connection.execute_batch("COMMIT")?;
        self.finished = true;
        self.connection.flush_changes();
        Ok(())
    }

    pub fn rollback(mut self) -> Result<()> {
        self.connection.execute_batch("ROLLBACK")?;
        self.connection.truncate_changes(0);
        self.finished = true;
        Ok(())
    }
}

impl Deref for Transaction<'_> {
    type Target = Connection;
    fn deref(&self) -> &Self::Target {
        self.connection
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            if let Err(error) = self.connection.execute_batch("ROLLBACK") {
                log::error!("rollback proprdb transaction: {error}");
            }
            self.connection.truncate_changes(0);
        }
    }
}

pub struct ChangeReceiver<T> {
    receiver: mpsc::Receiver<RawChange>,
    data: PhantomData<T>,
}

impl<T: Message + Default> ChangeReceiver<T> {
    fn decode(change: RawChange) -> Result<Change<T>> {
        Ok(match change.data {
            Some(bytes) => Change::Upsert(Row {
                id: change.id,
                at_ns: change.at_ns,
                data: T::decode(bytes.as_slice())?,
            }),
            None => Change::Delete {
                id: change.id,
                at_ns: change.at_ns,
            },
        })
    }

    pub fn recv(&self) -> Result<Change<T>> {
        Self::decode(self.receiver.recv()?)
    }
    pub fn try_recv(&self) -> Result<Change<T>> {
        Self::decode(self.receiver.try_recv()?)
    }
}
