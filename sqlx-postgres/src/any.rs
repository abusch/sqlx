use crate::{
    Either, PgArguments, PgColumn, PgConnection, PgQueryResult, PgRow, PgTransactionManager,
    PgTypeInfo, Postgres,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{TryFutureExt, TryStreamExt};
use std::borrow::Cow;
use std::sync::Arc;

use sqlx_core::any::{
    Any, AnyArguments, AnyColumn, AnyConnectionBackend, AnyQueryResult, AnyRow, AnyStatement,
    AnyTypeInfo, AnyTypeInfoKind, AnyValue, AnyValueKind,
};

use crate::type_info::PgType;
use sqlx_core::any::driver::AnyDriver;
use sqlx_core::column::Column;
use sqlx_core::connection::Connection;
use sqlx_core::describe::Describe;
use sqlx_core::executor::Executor;
use sqlx_core::ext::ustr::UStr;
use sqlx_core::row::Row;
use sqlx_core::transaction::TransactionManager;

pub const DRIVER: AnyDriver = AnyDriver::with_migrate::<Postgres>();

impl AnyConnectionBackend for PgConnection {
    fn name(&self) -> &str {
        "PostgreSQL"
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close(*self)
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close_hard(*self)
    }

    fn ping(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::pin(self)
    }

    fn begin(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        PgTransactionManager::begin(self)
    }

    fn commit(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        PgTransactionManager::commit(self)
    }

    fn rollback(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        PgTransactionManager::rollback(self)
    }

    fn start_rollback(&mut self) {
        PgTransactionManager::start_rollback(self)
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
            self.run(query, args, 0, persistent, None)
                .try_flatten_stream()
                .map(
                    move |res: sqlx_core::Result<Either<PgQueryResult, PgRow>>| match res? {
                        Either::Left(result) => Ok(Either::Left(map_result(result))),
                        Either::Right(row) => Ok(Either::Right(AnyRow::try_from(&row)?)),
                    },
                ),
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
            let mut stream = self.run(query, args, 1, persistent, None).await?;
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

            let parameters = if !statement.metadata.parameters.is_empty() {
                Some(Either::Left(
                    statement
                        .metadata
                        .parameters
                        .iter()
                        .enumerate()
                        .map(|(i, type_info)| {
                            map_type(type_info).ok_or_else(|| {
                                sqlx_core::Error::AnyDriverError(
                                    format!(
                                        "Any driver does not support type {} of parameter {}",
                                        type_info, i
                                    )
                                    .into(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ))
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

impl<'a> TryFrom<&'a PgTypeInfo> for AnyTypeInfo {
    type Error = sqlx_core::Error;

    fn try_from(pg_type: &'a PgTypeInfo) -> Result<Self, Self::Error> {
        Ok(AnyTypeInfo {
            kind: match &pg_type.0 {
                PgType::Void => AnyTypeInfoKind::Null,
                PgType::Int2 => AnyTypeInfoKind::SmallInt,
                PgType::Int4 => AnyTypeInfoKind::Integer,
                PgType::Int8 => AnyTypeInfoKind::BigInt,
                PgType::Float4 => AnyTypeInfoKind::Real,
                PgType::Float8 => AnyTypeInfoKind::Double,
                PgType::Bytea => AnyTypeInfoKind::Blob,
                PgType::Text => AnyTypeInfoKind::Text,
                _ => {
                    return Err(sqlx_core::Error::AnyDriverError(
                        format!(
                            "Any driver does not support the Postgres type {:?}",
                            pg_type
                        )
                        .into(),
                    ))
                }
            },
        })
    }
}

impl<'a> TryFrom<&'a PgColumn> for AnyColumn {
    type Error = sqlx_core::Error;

    fn try_from(col: &'a PgColumn) -> Result<Self, Self::Error> {
        let type_info = AnyTypeInfo::try_from(&col.type_info).ok_or_else(|e| {
            sqlx_core::Error::ColumnDecode {
                index: col.name.to_string(),
                source: e.into(),
            }
        })?;

        Ok(AnyColumn {
            ordinal: col.ordinal,
            name: col.name.clone(),
            type_info,
        })
    }
}

impl<'a> TryFrom<&'a PgRow> for AnyRow {
    type Error = sqlx_core::Error;

    fn try_from(row: &'a PgRow) -> Result<Self, Self::Error> {
        AnyRow::map_from(row, row.metadata.column_names.clone())
    }
}

fn map_result(res: PgQueryResult) -> AnyQueryResult {
    AnyQueryResult {
        rows_affected: res.rows_affected(),
        last_insert_id: None,
    }
}
