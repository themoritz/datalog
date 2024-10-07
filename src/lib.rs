#![allow(dead_code, unused_macros)]
#![feature(float_next_up_down)]

pub mod movies;

// Store

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    fmt::Display,
};

use ordered_float::NotNan;

pub trait Data: PartialEq + Clone {
    fn compare_to_bound(&self, bound: &Value) -> bool {
        self.clone().embed() == *bound
    }

    fn embed(self) -> Value;
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Debug)]
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

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub struct Attribute(pub String);

impl Attribute {
    fn min() -> Self {
        Attribute("".to_string())
    }

    fn next(&self) -> Self {
        let mut a = self.0.clone();
        a.push('\0');
        Attribute(a)
    }
}

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

/// "DatomEAV"
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
struct Datom {
    e: Entity,
    a: Attribute,
    v: Value,
}

impl From<Datom> for DatomAEV {
    fn from(value: Datom) -> Self {
        DatomAEV {
            a: value.a,
            e: value.e,
            v: value.v,
        }
    }
}

impl From<Datom> for DatomAVE {
    fn from(value: Datom) -> Self {
        DatomAVE {
            a: value.a,
            v: value.v,
            e: value.e,
        }
    }
}

impl From<DatomAEV> for Datom {
    fn from(value: DatomAEV) -> Self {
        Datom {
            e: value.e,
            a: value.a,
            v: value.v,
        }
    }
}

impl From<DatomAVE> for Datom {
    fn from(value: DatomAVE) -> Self {
        Datom {
            e: value.e,
            a: value.a,
            v: value.v,
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
struct DatomAEV {
    a: Attribute,
    e: Entity,
    v: Value,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
struct DatomAVE {
    a: Attribute,
    v: Value,
    e: Entity,
}

pub struct Store {
    eav: BTreeSet<Datom>,
    aev: BTreeSet<DatomAEV>, // TODO: In what situations do I need this?
    ave: BTreeSet<DatomAVE>,
    // TODO: vae for graph traversals?
}

impl Store {
    pub fn new() -> Self {
        Store {
            eav: BTreeSet::new(),
            aev: BTreeSet::new(),
            ave: BTreeSet::new(),
        }
    }

    fn insert(&mut self, datom: Datom) {
        self.eav.insert(datom.clone());
        self.aev.insert(datom.clone().into());
        self.ave.insert(datom.into());
    }

    fn into_iter(&self) -> impl Iterator<Item = Datom> {
        self.eav.clone().into_iter()
    }

    fn iter(&self) -> impl Iterator<Item = &Datom> + '_ {
        self.eav.iter()
    }

    fn iter_entity(&self, e: Entity) -> impl Iterator<Item = Datom> + '_ {
        let min = Datom {
            e,
            a: Attribute::min(),
            v: Value::min(),
        };
        let max = Datom {
            e: e.next(),
            a: Attribute::min(),
            v: Value::min(),
        };
        self.eav.range(min..max).map(|eav| eav.clone().into())
    }

    fn iter_entity_attribute(&self, e: Entity, a: Attribute) -> impl Iterator<Item = Datom> + '_ {
        let min = Datom {
            e,
            a: a.clone(),
            v: Value::min(),
        };
        let max = Datom {
            e,
            a: a.next(),
            v: Value::min(),
        };
        self.eav.range(min..max).map(|eav| eav.clone().into())
    }

    fn iter_attribute_value(&self, a: Attribute, v: Value) -> impl Iterator<Item = Datom> + '_ {
        let min = DatomAVE {
            a: a.clone(),
            v: v.clone(),
            e: Entity::min(),
        };
        let max = DatomAVE {
            a: a.clone(),
            v: v.next(),
            e: Entity::min(),
        };
        self.ave.range(min..max).map(|ave| ave.clone().into())
    }
}

impl FromIterator<Datom> for Store {
    fn from_iter<T: IntoIterator<Item = Datom>>(iter: T) -> Self {
        let mut s = Self::new();
        for datom in iter {
            s.insert(datom);
        }
        s
    }
}

// Query

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct Var(pub String);

impl Display for Var {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        ("?".to_string() + &self.0).fmt(f)
    }
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub find: Vec<Var>,
    pub where_: Where,
}

impl Query {
    pub fn qeval(&self, store: &Store) -> Result<HashSet<Vec<Value>>, String> {
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
pub enum Where {
    Pattern(Pattern),
    And(Box<Where>, Box<Where>),
    Or(Box<Where>, Box<Where>),
}

impl Where {
    pub fn and(l: Where, r: Where) -> Self {
        Self::And(Box::new(l), Box::new(r))
    }

    pub fn or(l: Where, r: Where) -> Self {
        Self::Or(Box::new(l), Box::new(r))
    }

    fn qeval<'a>(
        &'a self,
        store: &'a Store,
        frames: impl Iterator<Item = Frame> + 'a,
    ) -> Box<dyn Iterator<Item = Frame> + 'a> {
        match self {
            Where::Pattern(pattern) => Box::new(PatternI {
                pattern,
                frames,
                current_frame: None,
                store,
                candidates: Candidates::Strict(Box::new(Vec::new().into_iter())),
            }),
            Where::And(left, right) => right.qeval(store, left.qeval(store, frames)),
            Where::Or(left, right) => Box::new(OrI {
                inner: frames,
                store,
                left,
                right,
                queue: VecDeque::new(),
            }),
        }
    }
}

enum Candidates<'a> {
    Lazy(Box<dyn Iterator<Item = &'a Datom> + 'a>),
    Strict(Box<dyn Iterator<Item = Datom> + 'a>),
}

struct PatternI<'a, I> {
    pattern: &'a Pattern,
    frames: I,
    current_frame: Option<Frame>,
    store: &'a Store,
    candidates: Candidates<'a>,
}

impl<'a, I: Iterator<Item = Frame>> PatternI<'a, I> {
    fn match_(&mut self, mut frame: Frame, datom: &Datom) -> Option<Frame> {
        if let Ok(()) = self.pattern.match_(&mut frame, datom) {
            Some(frame)
        } else {
            self.next()
        }
    }

    fn next_frame(&mut self) -> Option<Frame> {
        match self.frames.next() {
            Some(frame) => {
                self.current_frame = Some(frame.clone());
                if let Some((entity, attribute)) = self.pattern.entity_attribute_bound(&frame) {
                    self.candidates = Candidates::Strict(Box::new(
                        self.store.iter_entity_attribute(entity, attribute),
                    ));
                } else if let Some(entity) = self.pattern.entity_bound(&frame) {
                    self.candidates = Candidates::Strict(Box::new(self.store.iter_entity(entity)));
                } else if let Some((attribute, value)) = self.pattern.attribute_value_bound(&frame)
                {
                    self.candidates = Candidates::Strict(Box::new(
                        self.store.iter_attribute_value(attribute, value),
                    ));
                } else {
                    self.candidates = Candidates::Lazy(Box::new(self.store.iter()));
                }
                self.next()
            }
            None => None,
        }
    }
}

impl<'a, I: Iterator<Item = Frame>> Iterator for PatternI<'a, I> {
    type Item = Frame;

    // TODO: Avoid recursion
    fn next(&mut self) -> Option<Self::Item> {
        match self.current_frame {
            Some(ref frame) => match self.candidates {
                Candidates::Lazy(ref mut i) => match i.next() {
                    Some(datom) => self.match_(frame.clone(), datom),
                    None => self.next_frame(),
                },
                Candidates::Strict(ref mut i) => match i.next() {
                    Some(datom) => self.match_(frame.clone(), &datom),
                    None => self.next_frame(),
                },
            },
            None => self.next_frame(),
        }
    }
}

struct OrI<'a, I> {
    inner: I,
    store: &'a Store,
    left: &'a Box<Where>,
    right: &'a Box<Where>,
    queue: VecDeque<Frame>,
}

impl<'a, I: Iterator<Item = Frame>> Iterator for OrI<'a, I> {
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
                self.next() // TODO: Avoid recursion
            } else {
                None
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Entry<T> {
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
pub struct Pattern {
    pub e: Entry<Entity>,
    pub a: Entry<Attribute>,
    pub v: Entry<Value>,
}

impl Pattern {
    fn match_(&self, frame: &mut Frame, datom: &Datom) -> Result<(), ()> {
        self.e.match_(frame, &datom.e)?;
        self.a.match_(frame, &datom.a)?;
        self.v.match_(frame, &datom.v)?;
        Ok(())
    }

    fn entity_bound(&self, frame: &Frame) -> Option<Entity> {
        match self.e {
            Entry::Lit(ref e) => Some(e.clone()),
            Entry::Var(ref v) => {
                let val = frame.bound.get(v)?;
                match val {
                    Value::Int(e) => Some(Entity(*e)),
                    _ => None,
                }
            }
        }
    }

    fn attribute_bound(&self, frame: &Frame) -> Option<Attribute> {
        match self.a {
            Entry::Lit(ref a) => Some(a.clone()),
            Entry::Var(ref v) => {
                let val = frame.bound.get(v)?;
                match val {
                    Value::Str(s) => Some(Attribute(s.clone())),
                    _ => None,
                }
            }
        }
    }

    fn value_bound(&self, frame: &Frame) -> Option<Value> {
        match self.v {
            Entry::Lit(ref v) => Some(v.clone()),
            Entry::Var(ref v) => frame.bound.get(v).cloned(),
        }
    }

    fn entity_attribute_bound(&self, frame: &Frame) -> Option<(Entity, Attribute)> {
        let e = self.entity_bound(frame)?;
        let a = self.attribute_bound(frame)?;
        Some((e, a))
    }

    fn attribute_value_bound(&self, frame: &Frame) -> Option<(Attribute, Value)> {
        let a = self.attribute_bound(frame)?;
        let v = self.value_bound(frame)?;
        Some((a, v))
    }
}

// Frame

#[derive(Clone, Debug)]
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

#[macro_export]
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

#[macro_export]
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
        HashSet::from_iter(vec![$(row!($row),)*])
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
        Store::from_iter(vec![
            datom![100, :name "Moritz"],
            datom![100, :age 39],
            datom![150, :name "Moritz"],
            datom![150, :age 30],
            datom![200, :name "Piet"],
            datom![200, :age 39],
        ])
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

        assert_eq!(q.qeval(&store()).unwrap(), table![[100, "Moritz"]]);
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

        assert_eq!(q.qeval(&store()).unwrap(), table![[200], [100]]);
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

        assert_eq!(q.qeval(&STORE).unwrap(), table![[1979]]);
    }

    #[test]
    fn movies_200() {
        let q = query! {
            find: [?attr, ?value],
            where: [
                [200, ?attr ?value]
            ]
        };

        assert_eq!(
            q.qeval(&STORE).unwrap(),
            table![
                ["movie/title", "The Terminator"],
                ["movie/year", 1984],
                ["movie/director", 100],
                ["movie/cast", 101],
                ["movie/cast", 102],
                ["movie/cast", 103],
                ["movie/sequel", 207]
            ]
        );
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
        assert_eq!(
            result,
            table![
                ["James Cameron", "The Terminator"],
                ["John McTiernan", "Predator"],
                ["Mark L. Lester", "Commando"],
                ["James Cameron", "Terminator 2: Judgment Day"],
                ["Jonathan Mostow", "Terminator 3: Rise of the Machines"]
            ]
        );
    }

    #[test]
    fn movies_1985() {
        let q = query! {
            find: [?title],
            where: [
                [?m, :movie/title ?title]
                [?m, :movie/year 1985]
            ]
        };

        assert_eq!(
            q.qeval(&STORE).unwrap(),
            table![
                ["Commando"],
                ["Rambo: First Blood Part II"],
                ["Mad Max Beyond Thunderdome"]
            ]
        );
    }

    #[test]
    fn movies_attrs() {
        let q = query! {
            find: [?attr],
            where: [
                [?m, :movie/title "Commando"]
                    [?m, ?attr ?v]
            ]
        };

        assert_eq!(
            q.qeval(&STORE).unwrap(),
            table![
                ["movie/director"],
                ["movie/cast"],
                ["trivia"],
                ["movie/year"],
                ["movie/title"]
            ]
        );
    }
}
