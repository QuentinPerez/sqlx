use crate::{
    Sqlite, SqliteConnection, SqliteQueryResult, SqliteRow, SqliteStatement, SqliteTypeInfo,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use sqlx_core::describe::Describe;
use sqlx_core::error::Error;
use sqlx_core::executor::{Execute, Executor};
use sqlx_core::Either;

impl<'c> Executor<'c> for &'c mut SqliteConnection {
    type Database = Sqlite;

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<SqliteQueryResult, SqliteRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
    {
        let sql = query.sql();
        let arguments = query.take_arguments();
        let persistent = query.persistent() && arguments.is_some();

        let iter = self.worker.execute(sql, arguments, persistent);

        futures_util::stream::iter(iter).boxed()
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        mut query: E,
    ) -> BoxFuture<'e, Result<Option<SqliteRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
    {
        let sql = query.sql();
        let arguments = query.take_arguments();
        let persistent = query.persistent() && arguments.is_some();

        Box::pin(async move {
            let item = self.worker.execute(sql, arguments, persistent).next();

            if let Some(Ok(Either::Right(row))) = item {
                return Ok(Some(row));
            }

            Ok(None)
        })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        _parameters: &[SqliteTypeInfo],
    ) -> BoxFuture<'e, Result<SqliteStatement<'q>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let statement = self.worker.prepare(sql)?;

            Ok(SqliteStatement {
                sql: sql.into(),
                ..statement
            })
        })
    }

    #[doc(hidden)]
    fn describe<'e, 'q: 'e>(self, sql: &'q str) -> BoxFuture<'e, Result<Describe<Sqlite>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move { self.worker.describe(sql) })
    }
}
