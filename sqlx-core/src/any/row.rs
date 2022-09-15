use crate::any::error::mismatched_types;
use crate::any::{Any, AnyColumn, AnyTypeInfo, AnyTypeInfoKind, AnyValue, AnyValueKind};
use crate::column::{Column, ColumnIndex};
use crate::database::{Database, HasValueRef};
use crate::decode::Decode;
use crate::error::Error;
use crate::ext::ustr::UStr;
use crate::row::Row;
use crate::type_info::TypeInfo;
use crate::types::Type;
use crate::value::{Value, ValueRef};
use std::sync::Arc;

#[derive(Clone)]
pub struct AnyRow {
    #[doc(hidden)]
    pub column_names: Arc<crate::HashMap<UStr, usize>>,
    #[doc(hidden)]
    pub columns: Vec<AnyColumn>,
    #[doc(hidden)]
    pub values: Vec<AnyValue>,
}

impl Row for AnyRow {
    type Database = Any;

    fn columns(&self) -> &[AnyColumn] {
        &self.columns
    }

    fn try_get_raw<I>(
        &self,
        index: I,
    ) -> Result<<Self::Database as HasValueRef<'_>>::ValueRef, Error>
    where
        I: ColumnIndex<Self>,
    {
        let index = index.index(self)?;
        Ok(self
            .columns
            .get(index)
            .ok_or_else(|| Error::ColumnIndexOutOfBounds {
                index,
                len: self.columns.len(),
            })?
            .value
            .as_ref())
    }

    fn try_get<'r, T, I>(&'r self, index: I) -> Result<T, Error>
    where
        I: ColumnIndex<Self>,
        T: Decode<'r, Self::Database> + Type<Self::Database>,
    {
        let value = self.try_get_raw(&index)?;
        let ty = value.type_info();

        if !value.is_null() && !ty.is_null() && !T::compatible(&ty) {
            Err(mismatched_types::<T>(&ty))
        } else {
            T::decode(value)
        }
        .map_err(|source| Error::ColumnDecode {
            index: format!("{:?}", index),
            source,
        })
    }
}

impl<'i> ColumnIndex<AnyRow> for &'i str {
    fn index(&self, row: &AnyRow) -> Result<usize, Error> {
        row.column_names
            .get(*self)
            .copied()
            .ok_or_else(|| Error::ColumnNotFound(self.to_string()))
    }
}

impl AnyRow {
    // This is not a `TryFrom` impl because trait impls are easy for users to accidentally
    // become reliant upon, even if hidden, but we want to be able to change the bounds
    // on this function as the `Any` driver gains support for more types.
    //
    // Also `column_names` needs to be passed by the driver to avoid making deep copies.
    #[doc(hidden)]
    pub fn map_from<'a, R: Row>(
        row: &'a R,
        column_names: Arc<crate::HashMap<UStr, usize>>,
    ) -> Result<Self, Error>
    where
        usize: ColumnIndex<R>,
        AnyTypeInfo: for<'a> TryFrom<&'a <R::Database as Database>::TypeInfo, Error = Error>,
        AnyColumn: for<'a> TryFrom<&'a <R::Database as Database>::Column, Error = Error>,
        i16: Type<R::Database> + Decode<'a, R::Database>,
        i32: Type<R::Database> + Decode<'a, R::Database>,
        i64: Type<R::Database> + Decode<'a, R::Database>,
        f32: Type<R::Database> + Decode<'a, R::Database>,
        f64: Type<R::Database> + Decode<'a, R::Database>,
        String: Type<R::Database> + Decode<'a, R::Database>,
        Vec<u8>: Type<R::Database> + Decode<'a, R::Database>,
    {
        let mut row_out = AnyRow {
            column_names,
            columns: Vec::with_capacity(row.columns().len()),
            values: Vec::with_capacity(row.columns().len()),
        };

        for col in row.columns() {
            let i = col.ordinal;

            let any_col = AnyColumn::try_from(col)?;

            let value_kind = match any_col.type_info.kind() {
                _ if row.data.values[i].is_none() => AnyValueKind::Null,
                AnyTypeInfoKind::Null => AnyValueKind::Null,
                AnyTypeInfoKind::SmallInt => AnyValueKind::SmallInt(row.try_get(i)?),
                AnyTypeInfoKind::Integer => AnyValueKind::Integer(row.try_get(i)?),
                AnyTypeInfoKind::BigInt => AnyValueKind::BigInt(row.try_get(i)?),
                AnyTypeInfoKind::Real => AnyValueKind::Real(row.try_get(i)?),
                AnyTypeInfoKind::Double => AnyValueKind::Double(row.try_get(i)?),
                AnyTypeInfoKind::Blob => AnyValueKind::Blob(row.try_get(i)?),
                AnyTypeInfoKind::Text => AnyValueKind::Text(row.try_get(i)?),
            };

            row_out.columns.push(any_col);
            row_out.values.push(AnyValue { kind: value_kind });
        }

        Ok(row_out)
    }
}
