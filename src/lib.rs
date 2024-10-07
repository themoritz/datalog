#![allow(dead_code, unused_macros)]

mod movies;

// Store

use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
};

trait Data: PartialEq + Clone {
    fn compare_to_bound(&self, bound: &Value) -> bool {
        self.clone().embed() == *bound
    }

    fn embed(self) -> Value;
}

#[derive(PartialEq, Clone, Debug)]
struct Entity(u64);

impl Data for Entity {
    fn embed(self) -> Value {
        Value::Int(self.0)
    }
}

#[derive(PartialEq, Clone, Debug)]
struct Attribute(String);

impl Data for Attribute {
    fn embed(self) -> Value {
        Value::Str(self.0)
    }
}

#[derive(PartialEq, Clone, Debug)]
enum Value {
    Str(String),
    Float(f64),
    Int(u64),
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(value)
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

#[derive(PartialEq, Debug, Clone)]
struct Datom {
    e: Entity,
    a: Attribute,
    v: Value,
}

struct Store {
    data: Vec<Datom>,
}

impl Store {
    fn into_iter(self) -> impl Iterator<Item = Datom> {
        self.data.into_iter()
    }

    fn iter(&self) -> impl Iterator<Item = &Datom> + '_ {
        self.data.iter()
    }
}

// Query

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct Var(String);

impl Display for Var {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        ("?".to_string() + &self.0).fmt(f)
    }
}

#[derive(Debug, PartialEq)]
struct Query {
    find: Vec<Var>,
    where_: Where,
}

impl Query {
    fn qeval(&self, store: &Store) -> Result<Vec<Vec<Value>>, String> {
        let frame_iter = self.where_.qeval(store, vec![Frame::new()].into_iter());
        frame_iter.map(|frame| frame.row(&self.find)).collect()
    }

    fn print_result(&self, store: &Store) {
        match self.qeval(store) {
            Ok(table) => {
                for var in &self.find {
                    print!("{:>25}", var);
                }
                println!();
                for _ in &self.find {
                    print!("-------------------------");
                }
                println!();
                for row in table {
                    for val in row {
                        print!("{:>25}", val);
                    }
                    println!();
                }
            }
            Err(e) => println!("Error: {e}"),
        }
    }
}

#[derive(Debug, PartialEq)]
enum Where {
    Pattern(Pattern),
    And(Box<Where>, Box<Where>),
    Or(Box<Where>, Box<Where>),
}

impl Where {
    fn and(l: Where, r: Where) -> Self {
        Self::And(Box::new(l), Box::new(r))
    }

    fn or(l: Where, r: Where) -> Self {
        Self::Or(Box::new(l), Box::new(r))
    }

    fn qeval<'a>(
        &'a self,
        store: &'a Store,
        frames: impl Iterator<Item = Frame> + 'a,
    ) -> Box<dyn Iterator<Item = Frame> + 'a> {
        match self {
            Where::Pattern(p) => Box::new(frames.flat_map(move |frame| {
                store.iter().filter_map(move |datom| {
                    let mut frame = frame.clone();
                    if let Ok(()) = p.match_(&mut frame, datom) {
                        Some(frame)
                    } else {
                        None
                    }
                })
            })),
            Where::And(left, right) => right.qeval(store, left.qeval(store, frames)),
            Where::Or(left, right) => Box::new(QevalOr {
                inner: frames,
                store,
                left,
                right,
                queue: VecDeque::new(),
            }),
        }
    }
}

struct QevalOr<'a, I> {
    inner: I,
    store: &'a Store,
    left: &'a Box<Where>,
    right: &'a Box<Where>,
    queue: VecDeque<Frame>,
}

impl<'a, I: Iterator<Item = Frame>> Iterator for QevalOr<'a, I> {
    type Item = Frame;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(frame) = self.queue.pop_front() {
            Some(frame)
        } else {
            if let Some(frame) = self.inner.next() {
                let left_frames = self.left.qeval(self.store, vec![frame.clone()].into_iter());
                let right_frames = self.right.qeval(self.store, vec![frame].into_iter());
                self.queue.extend(left_frames);
                self.queue.extend(right_frames);
                self.next()
            } else {
                None
            }
        }
    }
}

#[derive(Debug, PartialEq)]
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
    fn match_(&self, frame: &mut Frame, data: &T) -> Result<(), ()> {
        match self {
            Entry::Lit(lit) => {
                if *lit == *data {
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
                    frame.bind(var.clone(), data.clone().embed());
                    Ok(())
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
struct Pattern {
    e: Entry<Entity>,
    a: Entry<Attribute>,
    v: Entry<Value>,
}

impl Pattern {
    fn match_(&self, frame: &mut Frame, datom: &Datom) -> Result<(), ()> {
        self.e.match_(frame, &datom.e)?;
        self.a.match_(frame, &datom.a)?;
        self.v.match_(frame, &datom.v)?;
        Ok(())
    }
}

// Frame

#[derive(Clone)]
struct Frame {
    bound: HashMap<Var, Value>,
}

impl Frame {
    fn new() -> Self {
        Frame {
            bound: HashMap::new(),
        }
    }

    fn bind(&mut self, v: Var, val: Value) {
        self.bound.insert(v, val);
    }

    fn row(&self, vars: &[Var]) -> Result<Vec<Value>, String> {
        let mut result = Vec::new();
        for v in vars {
            if let Some(val) = self.bound.get(v) {
                result.push(val.clone());
            } else {
                return Err(format!("Variable not bound: ?{v}"));
            }
        }
        Ok(result)
    }
}

// Macros

macro_rules! query {
    {
        find: [ $(?$var:ident),* ],
        where: [ $($where_:tt)+ ]
    } => {{
        let exprs = vec![$(where_!($where_)),*];
        let mut iter = exprs.into_iter();
        let first = iter.next().unwrap();
        Query {
            find: vec![$(Var(stringify!($var).to_string()), )*],
            where_: iter.fold(first, |acc, e| Where::and(acc, e))
        }
    }};
}

macro_rules! where_ {
    ([ $e:expr, :$a:ident$(/$b:ident)* $v:expr ]) => {
        Where::Pattern(Pattern {
            e: Entry::Lit(Entity($e)),
            a: Entry::Lit(Attribute(concat!(stringify!($a) $(, "/", stringify!($b) )* ).to_string())),
            v: Entry::Lit(Value::from($v)),
        })
    };
    ([ $e:expr, :$a:ident$(/$b:ident)* ?$v:ident ]) => {
        Where::Pattern(Pattern {
            e: Entry::Lit(Entity($e)),
            a: Entry::Lit(Attribute(concat!(stringify!($a) $(, "/", stringify!($b) )* ).to_string())),
            v: Entry::Var(Var(stringify!($v).to_string())),
        })
    };
    ([ $e:expr, ?$a:ident $v:expr ]) => {
        Where::Pattern(Pattern {
            e: Entry::Lit(Entity($e)),
            a: Entry::Var(Var(stringify!($a).to_string())),
            v: Entry::Lit(Value::from($v)),
        })
    };
    ([ $e:expr, ?$a:ident ?$v:ident ]) => {
        Where::Pattern(Pattern {
            e: Entry::Lit(Entity($e)),
            a: Entry::Var(Var(stringify!($a).to_string())),
            v: Entry::Var(Var(stringify!($v).to_string())),
        })
    };
    ([ ?$e:ident, :$a:ident$(/$b:ident)* $v:expr ]) => {
        Where::Pattern(Pattern {
            e: Entry::Var(Var(stringify!($e).to_string())),
            a: Entry::Lit(Attribute(concat!(stringify!($a) $(, "/", stringify!($b) )* ).to_string())),
            v: Entry::Lit(Value::from($v)),
        })
    };
    ([ ?$e:ident, :$a:ident$(/$b:ident)* ?$v:ident ]) => {
        Where::Pattern(Pattern {
            e: Entry::Var(Var(stringify!($e).to_string())),
            a: Entry::Lit(Attribute(concat!(stringify!($a) $(, "/", stringify!($b) )* ).to_string())),
            v: Entry::Var(Var(stringify!($v).to_string())),
        })
    };
    ([ ?$e:ident, ?$a:ident $v:expr ]) => {
        Where::Pattern(Pattern {
            e: Entry::Var(Var(stringify!($e).to_string())),
            a: Entry::Var(Var(stringify!($a).to_string())),
            v: Entry::Lit(Value::from($v)),
        })
    };
    ([ ?$e:ident, ?$a:ident ?$v:ident ]) => {
        Where::Pattern(Pattern {
            e: Entry::Var(Var(stringify!($e).to_string())),
            a: Entry::Var(Var(stringify!($a).to_string())),
            v: Entry::Var(Var(stringify!($v).to_string())),
        })
    };
    ((or $($clause:tt)+)) => {{
        let exprs = vec![$(where_!($clause)),*];
        let mut iter = exprs.into_iter();
        let first = iter.next().unwrap();
        iter.fold(first, |acc, e| Where::or(acc, e))
    }};
    ((and $($clause:tt)+)) => {{
        let exprs = vec![$(where_!($clause)),*];
        let mut iter = exprs.into_iter();
        let first = iter.next().unwrap();
        iter.fold(first, |acc, e| Where::and(acc, e))
    }};
}

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

macro_rules! table {
    [ $($row:tt),* ] => {
        vec![$(row!($row),)*]
    };
}

macro_rules! row {
    ([ $($e:expr),* ]) => {
        vec![$(crate::Value::from($e),)*]
    };
}

#[cfg(test)]
mod tests {
    use movies::STORE;

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

    #[test]
    fn macro_query() {
        let expected = Query {
            find: vec![Var("a".to_string()), Var("b".to_string())],
            where_: Where::and(
                Where::and(
                    Where::and(
                        Where::and(
                            Where::and(
                                Where::Pattern(Pattern {
                                    e: Entry::Lit(Entity(3)),
                                    a: Entry::Lit(Attribute("foo".to_string())),
                                    v: Entry::Lit(Value::Int(3)),
                                }),
                                Where::Pattern(Pattern {
                                    e: Entry::Lit(Entity(1)),
                                    a: Entry::Lit(Attribute("bar".to_string())),
                                    v: Entry::Var(Var("a".to_string())),
                                }),
                            ),
                            Where::Pattern(Pattern {
                                e: Entry::Lit(Entity(0)),
                                a: Entry::Var(Var("b".to_string())),
                                v: Entry::Lit(Value::Str("M".to_string())),
                            }),
                        ),
                        Where::Pattern(Pattern {
                            e: Entry::Lit(Entity(0)),
                            a: Entry::Var(Var("b".to_string())),
                            v: Entry::Var(Var("a".to_string())),
                        }),
                    ),
                    Where::or(
                        Where::Pattern(Pattern {
                            e: Entry::Var(Var("x".to_string())),
                            a: Entry::Lit(Attribute("foo".to_string())),
                            v: Entry::Lit(Value::Int(3)),
                        }),
                        Where::Pattern(Pattern {
                            e: Entry::Var(Var("x".to_string())),
                            a: Entry::Lit(Attribute("bar".to_string())),
                            v: Entry::Var(Var("a".to_string())),
                        }),
                    ),
                ),
                Where::and(
                    Where::Pattern(Pattern {
                        e: Entry::Var(Var("x".to_string())),
                        a: Entry::Var(Var("b".to_string())),
                        v: Entry::Lit(Value::Str("M".to_string())),
                    }),
                    Where::Pattern(Pattern {
                        e: Entry::Var(Var("x".to_string())),
                        a: Entry::Var(Var("b".to_string())),
                        v: Entry::Var(Var("a".to_string())),
                    }),
                ),
            ),
        };

        let actual = query! {
            find: [?a, ?b],
            where: [
                [3, :foo 3]
                [1, :bar ?a]
                [0, ?b "M"]
                [0, ?b ?a]
                (or
                    [?x, :foo 3]
                    [?x, :bar ?a]
                )
                (and
                    [?x, ?b "M"]
                    [?x, ?b ?a]
                )
            ]
        };

        assert_eq!(actual, expected);
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
            .match_(&mut frame, &datom![100, :name "Moritz"])
            .unwrap();
        let row = frame.row(&[Var("a".to_string())]).unwrap();
        assert_eq!(row, vec![Value::Int(100)]);

        assert!(pattern.match_(&mut frame, &datom![100, :name 1]).is_err());
        assert!(pattern
            .match_(&mut frame, &datom![100, :name "Moritz"])
            .is_ok());
        assert!(pattern
            .match_(&mut frame, &datom![200, :name "Moritz"])
            .is_err());
    }

    fn store() -> Store {
        Store {
            data: vec![
                datom![100, :name "Moritz"],
                datom![100, :age 39],
                datom![150, :name "Moritz"],
                datom![150, :age 30],
                datom![200, :name "Piet"],
                datom![200, :age 39],
            ],
        }
    }

    #[test]
    fn qeval_pattern() {
        let q = query! {
            find: [?p, ?name],
            where: [
                [?p, :name "Moritz"]
                [?p, :name ?name]
                [?p, :age 39]
            ]
        };

        q.print_result(&store());
    }

    #[test]
    fn qeval_or() {
        let q = query! {
            find: [?e],
            where: [
                (or
                    [?e, :name "Piet"]
                    [?e, :name "Moritz"]
                )
                [?e, :age 39]
            ]
        };

        q.print_result(&store());
    }

    #[test]
    fn movies_alien() {
        let q = query! {
            find: [?year],
            where: [
              [?id, :movie/title "Alien"]
              [?id, :movie/year ?year]
            ]
        };

        let result = q.qeval(&STORE).unwrap();
        assert_eq!(result, table![
            [1979]
        ]);
    }

    #[test]
    fn movies_200() {
        let q = query! {
            find: [?attr, ?value],
            where: [
                [200, ?attr ?value]
            ]
        };

        let result = q.qeval(&STORE).unwrap();
        assert_eq!(result, table![
            ["movie/title", "The Terminator"],
            ["movie/year", 1984],
            ["movie/director", 100],
            ["movie/cast", 101],
            ["movie/cast", 102],
            ["movie/cast", 103],
            ["movie/sequel", 207]
        ]);
    }

    #[test]
    fn movies_arnold() {
        let q = query! {
            find: [?director, ?movie],
            where: [
                [?a, :person/name "Arnold Schwarzenegger"]
                [?m, :movie/cast ?a]
                [?m, :movie/title ?movie]
                [?m, :movie/director ?d]
                [?d, :person/name ?director]
            ]
        };

        let result = q.qeval(&STORE).unwrap();
        assert_eq!(result, table![
            ["James Cameron", "The Terminator"],
            ["John McTiernan", "Predator"],
            ["Mark L. Lester", "Commando"],
            ["James Cameron", "Terminator 2: Judgment Day"],
            ["Jonathan Mostow", "Terminator 3: Rise of the Machines"]
        ])
    }
}
