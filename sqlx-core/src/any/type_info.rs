use std::fmt::{self, Display, Formatter};

use crate::type_info::TypeInfo;

#[derive(Debug, Clone, PartialEq)]
pub struct AnyTypeInfo {
    #[doc(hidden)]
    pub kind: AnyTypeInfoKind,
}

impl AnyTypeInfo {
    pub fn kind(&self) -> AnyTypeInfoKind {
        self.kind
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnyTypeInfoKind {
    Null,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
    Text,
    Blob,
}

impl TypeInfo for AnyTypeInfo {
    fn is_null(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        use AnyTypeInfoKind::*;

        match self.kind {
            SmallInt => "SMALLINT",
            Integer => "INTEGER",
            BigInt => "BIGINT",
            Real => "REAL",
            Double => "DOUBLE",
            Text => "TEXT",
            Blob => "BLOB",
            Null => "NULL",
        }
    }
}

impl Display for AnyTypeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}
