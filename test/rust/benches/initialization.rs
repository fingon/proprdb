use std::{cell::Cell, time::Instant};

use proprdb_runtime::{Connection, Result};
use proprdb_rust_tests::system;

const FIXTURE: &str = include_str!("../../testdata/initialization.sql");
const ITERATIONS: u32 = 10;

thread_local! {
    static STATEMENT_COUNT: Cell<usize> = const { Cell::new(0) };
}

fn main() -> Result<()> {
    for storage in ["memory", "file"] {
        for row_count in [0, 1000, 10000] {
            let directory = tempfile::tempdir()?;
            let connection = if storage == "memory" {
                Connection::open_in_memory()?
            } else {
                Connection::open(directory.path().join("init.sqlite"))?
            };
            let crud = system::Crud::new(&connection);
            crud.initialize()?;
            let transaction = connection.transaction()?;
            for statement in FIXTURE.split(';').filter(|sql| !sql.trim().is_empty()) {
                if statement.contains('?') {
                    transaction.execute(statement, [row_count, row_count])?;
                } else {
                    transaction.execute(statement, [])?;
                }
            }
            transaction.commit()?;
            let started = Instant::now();
            for _ in 0..ITERATIONS {
                crud.initialize()?;
            }
            let elapsed_ns = started.elapsed().as_nanos() / u128::from(ITERATIONS);
            STATEMENT_COUNT.set(0);
            connection.trace_v2(
                rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
                Some(|event| {
                    if let rusqlite::trace::TraceEvent::Stmt(_, _) = event {
                        STATEMENT_COUNT.set(STATEMENT_COUNT.get() + 1);
                    }
                }),
            );
            crud.initialize()?;
            let statement_count = STATEMENT_COUNT.get();
            println!(
                "init rust storage={storage} rows={row_count} ns/op={elapsed_ns} statements/op={statement_count}"
            );
        }
    }
    Ok(())
}
