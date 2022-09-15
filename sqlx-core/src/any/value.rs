use std::borrow::Cow;
use std::marker::PhantomData;

use crate::any::error::mismatched_types;
use crate::any::{Any, AnyTypeInfo};
use crate::database::{Database, HasValueRef};
use crate::decode::Decode;
use crate::error::Error;
use crate::io::Encode;
use crate::type_info::TypeInfo;
use crate::types::Type;
use crate::value::{Value, ValueRef};

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AnyValueKind<'a> {
    Null,
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Real(f32),
    Double(f64),
    Text(Cow<'a, str>),
    Blob(Cow<'a, [u8]>),
}

#[derive(Clone, Debug)]
pub struct AnyValue {
    #[doc(hidden)]
    pub kind: AnyValueKind<'static>,
}

#[derive(Clone, Debug)]
pub struct AnyValueRef<'a> {
    pub(crate) kind: AnyValueKind<'a>,
}

impl Value for AnyValue {
    type Database = Any;

    fn as_ref(&self) -> <Self::Database as HasValueRef<'_>>::ValueRef {
        AnyValueRef {
            kind: match &self.kind {
                AnyValueKind::Null => AnyValueKind::Null,
                AnyValueKind::SmallInt(i) => AnyValueKind::SmallInt(*i),
                AnyValueKind::Integer(i) => AnyValueKind::Integer(*i),
                AnyValueKind::BigInt(i) => AnyValueKind::BigInt(*i),
                AnyValueKind::Real(r) => AnyValueKind::Real(*r),
                AnyValueKind::Double(d) => AnyValueKind::Double(*d),
                AnyValueKind::Text(t) => AnyValueKind::Text(Cow::Borrowed(t)),
                AnyValueKind::Blob(b) => AnyValueKind::Blob(Cow::Borrowed(b)),
            },
        }
    }

    fn type_info(&self) -> Cow<'_, <Self::Database as Database>::TypeInfo> {
        todo!()
    }

    fn is_null(&self) -> bool {
        false
    }
}

impl<'a> ValueRef<'a> for AnyValueRef<'a> {
    type Database = Any;

    fn to_owned(&self) -> <Self::Database as Database>::Value {
        AnyValue {
            kind: match &self.kind {
                AnyValueKind::Null => AnyValueKind::Null,
                AnyValueKind::SmallInt(i) => AnyValueKind::SmallInt(*i),
                AnyValueKind::Integer(i) => AnyValueKind::Integer(*i),
                AnyValueKind::BigInt(i) => AnyValueKind::BigInt(*i),
                AnyValueKind::Real(r) => AnyValueKind::Real(*r),
                AnyValueKind::Double(d) => AnyValueKind::Double(*d),
                AnyValueKind::Text(t) => AnyValueKind::Text(Cow::Owned(t.to_string())),
                AnyValueKind::Blob(b) => AnyValueKind::Blob(Cow::Owned(b.to_vec())),
            },
        }
    }

    fn type_info(&self) -> Cow<'_, <Self::Database as Database>::TypeInfo> {
        todo!()
    }

    fn is_null(&self) -> bool {
        false
    }
}
