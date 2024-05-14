use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use sqlx_core::describe::Describe;
use sqlx_core::error::Error;
use sqlx_core::transaction::{
    begin_ansi_transaction_sql, commit_ansi_transaction_sql, rollback_ansi_transaction_sql,
};
use sqlx_core::Either;

use crate::connection::describe::describe;
use crate::connection::establish::EstablishParams;
use crate::connection::execute;
use crate::connection::ConnectionState;
use crate::{Sqlite, SqliteArguments, SqliteQueryResult, SqliteRow, SqliteStatement};

// Each SQLite connection has a dedicated thread.

// TODO: Tweak this so that we can use a thread pool per pool of SQLite3 connections to reduce
//       OS resource usage. Low priority because a high concurrent load for SQLite3 is very
//       unlikely.

pub(crate) struct ConnectionWorker {
    pub(crate) conn: ConnectionState,
}

impl ConnectionWorker {
    pub(crate) async fn establish(params: EstablishParams) -> Result<Self, Error> {
        let conn = params.establish()?;

        return Ok(Self { conn });
    }

    pub(crate) fn prepare(&mut self, query: &str) -> Result<SqliteStatement<'static>, Error> {
        let conn = &mut self.conn;

        prepare(conn, &query).map(|prepared| prepared)
    }

    pub(crate) fn describe(&mut self, query: &str) -> Result<Describe<Sqlite>, Error> {
        let conn = &mut self.conn;

        describe(conn, query)
    }

    pub(crate) fn execute<'e>(
        &'e mut self,
        query: &'e str,
        args: Option<SqliteArguments<'e>>,
        persistent: bool,
    ) -> impl Iterator<Item = Result<Either<SqliteQueryResult, SqliteRow>, Error>> + 'e {
        let conn = &mut self.conn;

        let iter = execute::iter(
            conn,
            &query,
            args.map(SqliteArguments::into_static),
            persistent,
        );

        match iter {
            Ok(iter) => Either::Right(iter),
            Err(err) => Either::Left(std::iter::once(Err(err))),
        }
    }

    pub(crate) fn begin(&mut self) -> Result<(), Error> {
        let conn = &mut self.conn;
        let depth = conn.transaction_depth;

        conn.handle
            .exec(begin_ansi_transaction_sql(depth))
            .map(|_| {
                conn.transaction_depth += 1;
            })
    }

    pub(crate) fn commit(&mut self) -> Result<(), Error> {
        let conn = &mut self.conn;
        let depth = conn.transaction_depth;

        if depth > 0 {
            conn.handle
                .exec(commit_ansi_transaction_sql(depth))
                .map(|_| {
                    conn.transaction_depth -= 1;
                })
        } else {
            Ok(())
        }
    }

    pub(crate) fn rollback(&mut self) -> Result<(), Error> {
        let conn = &mut self.conn;
        let depth = conn.transaction_depth;

        if depth > 0 {
            conn.handle
                .exec(rollback_ansi_transaction_sql(depth))
                .map(|_| {
                    conn.transaction_depth -= 1;
                })
        } else {
            Ok(())
        }
    }

    pub(crate) fn clear_cache(&mut self) {
        self.conn.statements.clear();
    }
}

fn prepare(conn: &mut ConnectionState, query: &str) -> Result<SqliteStatement<'static>, Error> {
    // prepare statement object (or checkout from cache)
    let statement = conn.statements.get(query, true)?;

    let mut parameters = 0;
    let mut columns = None;
    let mut column_names = None;

    while let Some(statement) = statement.prepare_next(&mut conn.handle)? {
        parameters += statement.handle.bind_parameter_count();

        // the first non-empty statement is chosen as the statement we pull columns from
        if !statement.columns.is_empty() && columns.is_none() {
            columns = Some(Arc::clone(statement.columns));
            column_names = Some(Arc::clone(statement.column_names));
        }
    }

    Ok(SqliteStatement {
        sql: Cow::Owned(query.to_string()),
        columns: columns.unwrap_or_default(),
        column_names: column_names.unwrap_or_default(),
        parameters,
    })
}

fn update_cached_statements_size(conn: &ConnectionState, size: &AtomicUsize) {
    size.store(conn.statements.len(), Ordering::Release);
}
