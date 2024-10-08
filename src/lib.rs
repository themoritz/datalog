#![allow(dead_code, unused_macros)]
#![feature(float_next_up_down)]

use std::fmt::Display;

use ordered_float::NotNan;

#[cfg(any(test, feature = "bench"))]
pub mod movies;

pub mod query;
pub mod schema;
pub mod store;

type Result<T> = core::result::Result<T, String>;

pub trait Data: PartialEq + Clone {
    fn compare_to_bound(&self, bound: &Value) -> bool {
        self.clone().embed() == *bound
    }

    fn embed(self) -> Value;
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Hash, Clone, Debug)]
pub struct Entity(pub u64);

impl Entity {
    fn min() -> Self {
        Self(0)
    }

    fn next(&self) -> Self {
        Entity(self.0 + 1)
    }
}

impl Data for Entity {
    fn embed(self) -> Value {
        Value::Int(self.0)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub struct Attribute(pub String);

impl Data for Attribute {
    fn embed(self) -> Value {
        Value::Str(self.0)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub enum Value {
    Int(u64),
    Float(NotNan<f64>),
    Str(String),
}

impl Value {
    fn min() -> Self {
        Value::Int(0)
    }

    fn next(&self) -> Self {
        match self {
            Self::Int(i) => Self::Int(i + 1),
            Self::Float(f) => Self::Float(NotNan::new(f.to_owned().next_up()).unwrap()),
            Self::Str(s) => {
                let mut s2 = s.clone();
                s2.push('\0');
                Self::Str(s2)
            }
        }
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(NotNan::new(value).unwrap())
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::Int(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Str(value.to_string())
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Str(s) => s.fmt(f),
            Value::Float(x) => x.fmt(f),
            Value::Int(x) => x.fmt(f),
        }
    }
}

impl Data for Value {
    fn embed(self) -> Value {
        self
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct Datom {
    pub e: Entity,
    pub a: Attribute,
    pub v: Value,
}

// Macros

#[macro_export]
macro_rules! datom {
    [ $e:expr, :$a:ident$(/$b:ident)* $v:expr ] => {
        crate::Datom {
            e: crate::Entity($e),
            a: crate::Attribute(concat!(stringify!($a) $(, "/", stringify!($b) )* ).to_string()),
            v: crate::Value::from($v),
        }
    };
}

#[macro_export]
macro_rules! table {
    [ $($row:tt),* ] => {
        std::collections::HashSet::from_iter(vec![$(row!($row),)*])
    };
}

#[macro_export]
macro_rules! row {
    ([ $($e:expr),* ]) => {
        vec![$(crate::Value::from($e),)*]
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn macro_datom() {
        let datom = datom![20, :person/name "M"];
        assert_eq!(
            datom,
            Datom {
                e: Entity(20),
                a: Attribute("person/name".to_string()),
                v: Value::Str("M".to_string()),
            }
        )
    }
}
