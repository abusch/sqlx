//! Conversions between Rust and standard **SQL** types.
//!
//! # Types
//!
//! | Rust type                             | SQL type(s)                                          |
//! |---------------------------------------|------------------------------------------------------|
//! | `bool`                                | BOOLEAN                                              |
//! | `i16`                                 | SMALLINT                                             |
//! | `i32`                                 | INT                                                  |
//! | `i64`                                 | BIGINT                                               |
//! | `f32`                                 | FLOAT                                                |
//! | `f64`                                 | DOUBLE                                               |
//! | `&str`, [`String`]                    | VARCHAR, CHAR, TEXT                                  |
//!
//! # Nullable
//!
//! In addition, `Option<T>` is supported where `T` implements `Type`. An `Option<T>` represents
//! a potentially `NULL` value from SQL.

use crate::any::type_info::AnyTypeInfoKind;
use crate::any::value::AnyValueKind;
use crate::any::{Any, AnyTypeInfo, AnyValueRef};
use crate::database::{HasArguments, HasValueRef};
use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::Type;
use std::borrow::Cow;

impl<'a> Type<Any> for &'a str {
    fn type_info() -> AnyTypeInfo {
        AnyTypeInfo {
            kind: AnyTypeInfoKind::Text,
        }
    }
}

impl<'a> Encode<'a, Any> for &'a str {
    fn encode(self, buf: &mut <Any as HasArguments<'a>>::ArgumentBuffer) -> IsNull
    where
        Self: Sized,
    {
        buf.0.push(AnyValueKind::Text(self.into()));
        IsNull::No
    }

    fn encode_by_ref(&self, buf: &mut <Any as HasArguments<'a>>::ArgumentBuffer) -> IsNull {
        (*self).encode(buf)
    }
}

impl<'a> Decode<'a, Any> for &'a str {
    fn decode(value: <Any as HasValueRef<'a>>::ValueRef) -> Result<Self, BoxDynError> {
        match value.kind {
            AnyValueKind::Text(Cow::Borrowed(text)) => Ok(text),
            // This shouldn't happen in practice, it means the user got an `AnyValueRef`
            // constructed from an owned `String` which shouldn't be allowed by the API.
            AnyValueKind::Text(Cow::Owned(_text)) => {
                panic!("attempting to return a borrow that outlives its buffer")
            }
            other => Err(format!("expected string, got {:?}", other).into()),
        }
    }
}
