// Store

use std::{collections::HashMap, fmt::{Display, Pointer}};

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
    Float(f64)
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

struct Datom {
    e: Entity,
    a: Attribute,
    v: Value,
}

impl Datom {
    fn str(e: u64, a: impl ToString, v: impl ToString) -> Self {
        Datom { e: Entity(e), a: Attribute(a.to_string()), v: Value::Str(v.to_string()) }
    }

    fn flt(e: u64, a: impl ToString, v: f64) -> Self {
        Datom { e: Entity(e), a: Attribute(a.to_string()), v: Value::Float(v) }
    }
}

struct Store {
    data: Vec<Datom>
}

impl Store {
    fn into_iter(self) -> impl Iterator {
        self.data.into_iter()
    }
}

// Query

#[derive(PartialEq, Eq, Hash, Clone)]
struct Var(String);

impl Display for Var {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

enum Query {
    Pattern(Pattern),
    And(Box<Query>, Box<Query>),
    Or(Box<Query>, Box<Query>)
}

impl Query {
    fn and(l: Query, r: Query) -> Self {
        Self::And(Box::new(l), Box::new(r))
    }
}

enum Entry<T> {
    Lit(T),
    Var(Var)
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
    fn match_part(&self, frame: &mut Frame, data: T) -> Result<(), ()> {
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
    fn match_pattern(&self, frame: &mut Frame, datom: Datom) -> Result<(), ()> {
        self.e.match_part(frame, datom.e)?;
        self.a.match_part(frame, datom.a)?;
        self.v.match_part(frame, datom.v)?;
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
            },
            BoundValue::Value(v) => (*v).fmt(f),
        }
    }
}

struct Frame {
    bound: HashMap<Var, BoundValue>
}

impl Frame {
    fn new() -> Self {
        Frame {
            bound: HashMap::new()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> Store {
        Store {
            data: vec![
                Datom::str(100, "name", "Moritz"),
                Datom::flt(100, "age", 39.),
            ]
        }
    }

    #[test]
    fn pattern_match() {
        let pattern = Pattern {
            e: Entry::Var(Var("a".to_string())),
            a: Entry::Lit(Attribute("name".to_string())),
            v: Entry::Lit(Value::Str("Moritz".to_string())),
        };

        let mut frame = Frame::new();
        pattern.match_pattern(&mut frame, Datom::str(100, "name", "Moritz")).unwrap();
        let row = frame.row(&[Var("a".to_string())]).unwrap();
        assert_eq!(row, vec![BoundValue::Entity(Entity(100))]);

        assert!(pattern.match_pattern(&mut frame, Datom::flt(100, "name", 1.0)).is_err());
        assert!(pattern.match_pattern(&mut frame, Datom::str(100, "name", "Moritz")).is_ok());
        assert!(pattern.match_pattern(&mut frame, Datom::str(200, "name", "Moritz")).is_err());
    }
}
