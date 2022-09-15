use crate::protocol::text::ColumnType;
use crate::{
    MySql, MySqlColumn, MySqlConnection, MySqlQueryResult, MySqlRow, MySqlTransactionManager,
    MySqlTypeInfo,
};
use either::Either;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, TryFutureExt, TryStreamExt};
use sqlx_core::any::driver::AnyDriver;
use sqlx_core::any::{
    Any, AnyArguments, AnyColumn, AnyConnectionBackend, AnyQueryResult, AnyRow, AnyStatement,
    AnyTypeInfo, AnyTypeInfoKind,
};
use sqlx_core::connection::Connection;
use sqlx_core::describe::Describe;
use sqlx_core::executor::Executor;
use sqlx_core::transaction::TransactionManager;
use std::borrow::Cow;

pub const DRIVER: AnyDriver = AnyDriver::with_migrate::<MySql>();

impl AnyConnectionBackend for MySqlConnection {
    fn name(&self) -> &str {
        "MySQL"
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close(*self)
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close_hard(*self)
    }

    fn ping(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::ping(self)
    }

    fn begin(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        MySqlTransactionManager::begin(self)
    }

    fn commit(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        MySqlTransactionManager::commit(self)
    }

    fn rollback(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        MySqlTransactionManager::rollback(self)
    }

    fn start_rollback(&mut self) {
        MySqlTransactionManager::start_rollback(self)
    }

    fn flush(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::flush(self)
    }

    fn should_flush(&self) -> bool {
        Connection::should_flush(self)
    }

    fn fetch_many<'q>(
        &'q mut self,
        query: &'q str,
        arguments: Option<AnyArguments<'q>>,
    ) -> BoxStream<'q, sqlx_core::Result<Either<AnyQueryResult, AnyRow>>> {
        let persistent = arguments.is_some();
        let args = arguments.map(AnyArguments::convert_into);

        Box::pin(
            self.run(query, args, persistent)
                .try_flatten_stream()
                .map(|res| {
                    Ok(match res? {
                        Either::Left(result) => Either::Left(map_result(result)),
                        Either::Right(row) => Either::Right(AnyRow::try_from(&row)?),
                    })
                }),
        )
    }

    fn fetch_optional<'q>(
        &'q mut self,
        query: &'q str,
        arguments: Option<AnyArguments<'q>>,
    ) -> BoxFuture<'q, sqlx_core::Result<Option<AnyRow>>> {
        let persistent = arguments.is_some();
        let args = arguments.map(AnyArguments::convert_into);

        Box::pin(async move {
            let mut stream = self.run(query, args, persistent).await?;
            futures_util::pin_mut!(stream);

            if let Some(Either::Right(row)) = stream.try_next().await? {
                return Ok(Some(AnyRow::try_from(&row)?));
            }

            Ok(None)
        })
    }

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        sql: &'q str,
        _parameters: &[AnyTypeInfo],
    ) -> BoxFuture<'c, sqlx_core::Result<AnyStatement<'q>>> {
        Box::pin(async move {
            let statement = Executor::prepare_with(self, sql, &[]).await?;

            let parameters = if statement.metadata.parameters > 0 {
                Some(Either::Right(statement.metadata.parameters))
            } else {
                None
            };

            let columns = statement
                .metadata
                .columns
                .iter()
                .map(map_column)
                .collect::<Result<Vec<_>, _>>()?;

            Ok(AnyStatement {
                sql: Cow::Borrowed(sql),
                parameters,
                column_names: statement.metadata.column_names.clone(),
                columns,
            })
        })
    }

    fn describe<'q>(&'q mut self, sql: &'q str) -> BoxFuture<'q, sqlx_core::Result<Describe<Any>>> {
        Box::pin(async move {
            let describe = Executor::describe(self, sql).await?;

            let columns = describe
                .columns
                .iter()
                .map(map_column)
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Describe {
                columns,
                parameters: describe.parameters,
                nullable: describe.nullable,
            })
        })
    }
}

impl<'a> TryFrom<&'a MySqlTypeInfo> for AnyTypeInfo {
    type Error = sqlx_core::Error;

    fn try_from(type_info: &'a MySqlTypeInfo) -> Result<Self, Self::Error> {
        Ok(AnyTypeInfo {
            kind: match &mysql_type.r#type {
                ColumnType::Null => AnyTypeInfoKind::Null,
                ColumnType::Short => AnyTypeInfoKind::SmallInt,
                ColumnType::Long => AnyTypeInfoKind::Integer,
                ColumnType::LongLong => AnyTypeInfoKind::BigInt,
                ColumnType::Float => AnyTypeInfoKind::Real,
                ColumnType::Double => AnyTypeInfoKind::Double,
                ColumnType::Blob
                | ColumnType::TinyBlob
                | ColumnType::MediumBlob
                | ColumnType::LongBlob => AnyTypeInfoKind::Blob,
                ColumnType::String | ColumnType::VarString | ColumnType::VarChar => {
                    AnyTypeInfoKind::Text
                }
                _ => {
                    return Err(sqlx_core::Error::AnyDriverError(
                        format!("Any driver does not support MySql type {:?}", type_info).into(),
                    ))
                }
            },
        })
    }
}

impl<'a> TryFrom<&'a MySqlColumn> for AnyColumn {
    type Error = sqlx_core::Error;

    fn try_from(column: &'a MySqlColumn) -> Result<Self, Self::Error> {
        let type_info = AnyTypeInfo::try_from(&column.type_info)?;

        Ok(AnyColumn {
            ordinal: column.ordinal,
            name: column.name.clone(),
            type_info,
        })
    }
}

impl<'a> TryFrom<&'a MySqlRow> for AnyRow {
    type Error = sqlx_core::Error;

    fn try_from(row: &'a MySqlRow) -> Result<Self, Self::Error> {
        AnyRow::map_from(row, row.column_names.clone())
    }
}

fn map_result(result: MySqlQueryResult) -> AnyQueryResult {
    AnyQueryResult {
        rows_affected: result.rows_affected,
        last_insert_id: Some(result.last_insert_id as i64),
    }
}
