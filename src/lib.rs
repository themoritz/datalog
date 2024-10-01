#![allow(dead_code, unused_macros)]

// Store

use std::{collections::HashMap, fmt::Display};

trait Data: PartialEq {
    fn compare_to_bound(&self, bound: &BoundValue) -> bool;
    fn embed(self) -> BoundValue;
}

#[derive(PartialEq, Clone, Debug)]
struct Entity(u64);

impl Data for Entity {
    fn compare_to_bound(&self, bound: &BoundValue) -> bool {
        match bound {
            BoundValue::Entity(e) => e == self,
            _ => false,
        }
    }

    fn embed(self) -> BoundValue {
        BoundValue::Entity(self)
    }
}

#[derive(PartialEq, Clone, Debug)]
struct Attribute(String);

impl Data for Attribute {
    fn compare_to_bound(&self, bound: &BoundValue) -> bool {
        match bound {
            BoundValue::Attribute(a) => a == self,
            _ => false,
        }
    }

    fn embed(self) -> BoundValue {
        BoundValue::Attribute(self)
    }
}

#[derive(PartialEq, Clone, Debug)]
enum Value {
    Str(String),
    Float(f64),
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::Float(value as f64)
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
        }
    }
}

impl Data for Value {
    fn compare_to_bound(&self, bound: &BoundValue) -> bool {
        match bound {
            BoundValue::Value(v) => v == self,
            _ => false,
        }
    }

    fn embed(self) -> BoundValue {
        BoundValue::Value(self)
    }
}

#[derive(PartialEq, Debug)]
struct Datom {
    e: Entity,
    a: Attribute,
    v: Value,
}

struct Store {
    data: Vec<Datom>,
}

impl Store {
    fn into_iter(self) -> impl Iterator {
        self.data.into_iter()
    }
}

// Query

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct Var(String);

impl Display for Var {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

struct Query {
    find: Vec<Var>,
    where_: Where,
}

enum Where {
    Pattern(Pattern),
    And(Box<Where>, Box<Where>),
    Or(Box<Where>, Box<Where>),
}

impl Where {
    fn and(l: Where, r: Where) -> Self {
        Self::And(Box::new(l), Box::new(r))
    }
}

enum Entry<T> {
    Lit(T),
    Var(Var),
}

impl<T: Display> Display for Entry<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Entry::Lit(l) => l.fmt(f),
            Entry::Var(v) => v.fmt(f),
        }
    }
}

impl<T: Data> Entry<T> {
    fn match_(&self, frame: &mut Frame, data: T) -> Result<(), ()> {
        match self {
            Entry::Lit(lit) => {
                if *lit == data {
                    Ok(())
                } else {
                    Err(())
                }
            }
            Entry::Var(var) => {
                if let Some(bound_value) = frame.bound.get(&var) {
                    if data.compare_to_bound(bound_value) {
                        Ok(())
                    } else {
                        Err(())
                    }
                } else {
                    frame.bind(var.clone(), data.embed());
                    Ok(())
                }
            }
        }
    }
}

struct Pattern {
    e: Entry<Entity>,
    a: Entry<Attribute>,
    v: Entry<Value>,
}

impl Pattern {
    fn match_(&self, frame: &mut Frame, datom: Datom) -> Result<(), ()> {
        self.e.match_(frame, datom.e)?;
        self.a.match_(frame, datom.a)?;
        self.v.match_(frame, datom.v)?;
        Ok(())
    }
}

// Frame

#[derive(PartialEq, Clone, Debug)]
enum BoundValue {
    Entity(Entity),
    Attribute(Attribute),
    Value(Value),
}

impl Display for BoundValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BoundValue::Entity(Entity(e)) => (*e).fmt(f),
            BoundValue::Attribute(Attribute(a)) => {
                write!(f, ":")?;
                (*a).fmt(f)
            }
            BoundValue::Value(v) => (*v).fmt(f),
        }
    }
}

struct Frame {
    bound: HashMap<Var, BoundValue>,
}

impl Frame {
    fn new() -> Self {
        Frame {
            bound: HashMap::new(),
        }
    }

    fn bind(&mut self, v: Var, val: BoundValue) {
        self.bound.insert(v, val);
    }

    fn view(&self, vars: &[Var]) -> Result<(), String> {
        let row = self.row(vars)?;
        for v in row {
            print!("{:>10}", v);
        }
        println!();
        Ok(())
    }

    fn row(&self, vars: &[Var]) -> Result<Vec<BoundValue>, String> {
        let mut result = Vec::new();
        for v in vars {
            if let Some(val) = self.bound.get(v) {
                result.push(val.clone());
            } else {
                return Err("Variable not bound".to_string());
            }
        }
        Ok(result)
    }
}

// Macros

macro_rules! parse_var {
    (? $var:ident) => {
        Var(stringify!($var).to_string())
    };
}

macro_rules! parse_attr {
    (: $($var:tt)+) => {
        Attribute(stringify!($($var)+).to_string())
    };
}

macro_rules! parse_val {
    ($val:expr) => {
        Value::from($val)
    };
}

macro_rules! datom {
    [ $e:expr, :$a:ident $( / $b:ident )*, $v:expr ] => {
        Datom {
            e: Entity($e),
            a: Attribute(concat!(stringify!($a) $(, "/", stringify!($b) )* ).to_string()),
            v: Value::from($v),
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> Store {
        Store {
            data: vec![datom![100, :name, "Moritz"], datom![100, :age, 39.]],
        }
    }

    #[test]
    fn macro_datom() {
        let datom = datom![10 + 10, :person/name, "M"];
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
    fn pattern_match() {
        let pattern = Pattern {
            e: Entry::Var(Var("a".to_string())),
            a: Entry::Lit(Attribute("name".to_string())),
            v: Entry::Lit(Value::Str("Moritz".to_string())),
        };

        let mut frame = Frame::new();
        pattern
            .match_(&mut frame, datom![100, :name, "Moritz"])
            .unwrap();
        let row = frame.row(&[Var("a".to_string())]).unwrap();
        assert_eq!(row, vec![BoundValue::Entity(Entity(100))]);

        assert!(pattern.match_(&mut frame, datom![100, :name, 1.0]).is_err());
        assert!(pattern
            .match_(&mut frame, datom![100, :name, "Moritz"])
            .is_ok());
        assert!(pattern
            .match_(&mut frame, datom![200, :name, "Moritz"])
            .is_err());
    }
}
