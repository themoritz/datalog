#![allow(dead_code, unused_macros)]
#![feature(float_next_up_down)]

use std::fmt::Display;

use ordered_float::NotNan;
use pull::PullError;
use serde::{de::Visitor, Deserialize, Deserializer, Serialize};

#[cfg(any(test, feature = "bench"))]
pub mod movies;

pub mod mem_store;
pub mod parsers;
pub mod persist;
pub mod pg_store;
pub mod pull;
pub mod query;
pub mod store;
pub mod transact;

type Result<T> = core::result::Result<T, String>;

pub trait Data: PartialEq + Clone {
    fn compare_to_bound(&self, bound: &Value) -> bool {
        self.clone().embed() == *bound
    }

    fn embed(self) -> Value;
}

#[derive(
    PartialEq, Eq, PartialOrd, Ord, Copy, Hash, Clone, Debug, Default, Deserialize, Serialize,
)]
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
        Value::Ref(self.0)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub struct Attribute(pub String);

impl Data for Attribute {
    fn embed(self) -> Value {
        Value::Str(self.0)
    }
}

impl Display for Attribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for Attribute {
    fn from(value: &str) -> Self {
        Attribute(value.to_string())
    }
}

// Assuming Attribute can be deserialized from a string
impl<'de> Deserializer<'de> for &'de Attribute {
    type Error = PullError;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_str(&self.0)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf option unit
        unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier ignored_any
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Value {
    Bool(bool),
    Int(u64),
    Ref(u64),
    Float(NotNan<f64>),
    Str(String),
}

impl Value {
    fn min() -> Self {
        Value::Int(0)
    }

    // TODO: Make this safer to handle overflows
    fn next(&self) -> Self {
        match self {
            Self::Bool(b) => {
                if *b {
                    Self::Int(0)
                } else {
                    Self::Bool(true)
                }
            }
            Self::Int(i) => Self::Int(i + 1),
            Self::Ref(i) => Self::Ref(i + 1),
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

impl From<Entity> for Value {
    fn from(value: Entity) -> Self {
        Self::Ref(value.0)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Str(s) => s.fmt(f),
            Value::Ref(i) => i.fmt(f),
            Value::Float(x) => x.fmt(f),
            Value::Int(x) => x.fmt(f),
            Value::Bool(b) => b.fmt(f),
        }
    }
}

impl Data for Value {
    fn embed(self) -> Value {
        self
    }
}

struct Ref(u64);

impl From<Ref> for Value {
    fn from(value: Ref) -> Self {
        Value::Ref(value.0)
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
    ($e:expr, $a:literal = $v:expr) => {
        crate::Datom {
            e: crate::Entity($e),
            a: crate::Attribute($a.to_string()),
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
    use mem_store::MemStore;
    use pretty_assertions::assert_eq;
    use store::{Cardinality, Store, Type};
    use transact::Tmp;

    use super::*;

    #[test]
    fn macro_datom() {
        let datom = datom!(20, "person/name" = "M");
        assert_eq!(
            datom,
            Datom {
                e: Entity(20),
                a: Attribute("person/name".to_string()),
                v: Value::Str("M".to_string()),
            }
        )
    }

    #[test]
    fn integration() -> Result<()> {
        let mut store = MemStore::new();

        store.add_attribute("name", Type::Str, Cardinality::One, "The name")?;
        store.add_attribute("age", Type::Int, Cardinality::One, "Age")?;
        store.add_attribute("friend", Type::Ref, Cardinality::Many, "Friend")?;

        let tx = add!(Tmp("1"), {
            "name": "Moritz",
            "friend": {
                "name": "Piet"
            }
        });

        store.transact(tx)?;

        let q = query! {
            find: [?name],
            where: [
                (?p, "name" = ?name)
            ]
        };

        assert_eq!(q.qeval(&store)?, table![["Piet"], ["Moritz"]]);

        //

        let tx = retract!(14);

        store.transact(tx)?;

        assert_eq!(q.qeval(&store)?, table![["Piet"]]);

        Ok(())
    }
}
