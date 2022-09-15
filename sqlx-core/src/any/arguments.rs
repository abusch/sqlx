use crate::any::value::AnyValueKind;
use crate::any::{Any, AnyValueRef};
use crate::arguments::Arguments;
use crate::encode::Encode;
use crate::types::Type;
use std::marker::PhantomData;

pub struct AnyArguments<'q> {
    values: AnyArgumentBuffer<'q>,
}

impl<'q> Arguments<'q> for AnyArguments<'q> {
    type Database = Any;

    fn reserve(&mut self, additional: usize, _size: usize) {
        self.values.0.reserve(additional);
    }

    fn add<T>(&mut self, value: T)
    where
        T: 'q + Send + Encode<'q, Self::Database> + Type<Self::Database>,
    {
        value.encode(&mut self.values);
    }
}

pub struct AnyArgumentBuffer<'q>(pub(crate) Vec<AnyValueKind<'q>>);

impl<'q> Default for AnyArguments<'q> {
    fn default() -> Self {
        AnyArguments {
            values: AnyArgumentBuffer(vec![]),
        }
    }
}

impl<'q> AnyArguments<'q> {
    #[doc(hidden)]
    pub fn convert_into<A: Arguments<'q>>(self) -> A
    where
        Option<()>: Encode<'q, A::Database>,
        i16: Encode<'q, A::Database>,
        i32: Encode<'q, A::Database>,
        i64: Encode<'q, A::Database>,
        f32: Encode<'q, A::Database>,
        f64: Encode<'q, A::Database>,
        &'q str: Encode<'q, A::Database>,
        &'q [u8]: Encode<'q, A::Database>,
    {
        let mut out = A::default();

        for arg in self.values.0 {
            match arg {
                AnyValueKind::Null => out.add(Option::<()>::None),
                AnyValueKind::SmallInt(i) => out.add(i),
                AnyValueKind::Integer(i) => out.add(i),
                AnyValueKind::BigInt(i) => out.add(i),
                AnyValueKind::Real(r) => out.add(r),
                AnyValueKind::Double(d) => out.add(d),
                AnyValueKind::Text(t) => out.add(&**t),
                AnyValueKind::Blob(b) => out.add(&**b),
            }
        }

        out
    }
}
