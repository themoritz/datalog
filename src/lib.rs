#![allow(dead_code, unused_macros)]
#![feature(float_next_up_down)]

pub mod movies;

type Result<T> = core::result::Result<T, String>;

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

/// "DatomEAV"
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
struct Datom {
    e: Entity,
    a: Attribute,
    v: Value,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
struct EAV {
    e: Entity,
    a: Entity,
    v: Value,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
struct AVE {
    a: Entity,
    v: Value,
    e: Entity,
}

impl From<AVE> for EAV {
    fn from(value: AVE) -> Self {
        EAV {
            e: value.e,
            a: value.a,
            v: value.v,
        }
    }
}

pub struct Store {
    schema: Schema,
    eav: BTreeSet<EAV>,
    ave: BTreeSet<AVE>,
    next_id: u64,
    next_tx: u64,
}

impl Store {
    pub fn new() -> Self {
        Store {
            schema: Schema::new(),
            eav: BTreeSet::new(),
            ave: BTreeSet::new(),
            next_id: 1,
            next_tx: 1,
        }
    }

    fn add_attribute(&mut self, name: &str, type_: Type, cardinality: Cardinality) -> Result<()> {
        let a = Attribute(name.to_string());
        if self.schema.details.contains_key(&a) {
            return Err("Attribute already defined".to_string());
        }

        self.schema.ids.insert(a.clone(), Entity(self.next_id));
        self.schema
            .attributes
            .insert(Entity(self.next_id), a.clone());
        self.next_id += 1;

        let details = AttributeDetails { type_, cardinality };
        self.schema.details.insert(a, details);

        Ok(())
    }

    fn insert(&mut self, datom: Datom) -> Result<()> {
        let a = match self.schema.get_id(&datom.a) {
            Some(id) => Ok(id),
            None => Err(format!("Could not find id for attribute `{}`", datom.a.0)),
        }?;

        if !self.schema.valid_type(&datom.a, &datom.v) {
            return Err(format!(
                "Invalid type for attribute `{}`: {:?}",
                datom.a.0, datom.v
            ));
        }

        self.eav.insert(EAV {
            e: datom.e,
            a,
            v: datom.v.clone(),
        });
        self.ave.insert(AVE {
            a,
            v: datom.v.clone(),
            e: datom.e,
        });

        Ok(())
    }

    fn into_iter(&self) -> impl Iterator<Item = EAV> {
        self.eav.clone().into_iter()
    }

    fn iter(&self) -> impl Iterator<Item = &EAV> + '_ {
        self.eav.iter()
    }

    fn iter_entity(&self, e: Entity) -> impl Iterator<Item = EAV> + '_ {
        let min = EAV {
            e,
            a: Entity::min(),
            v: Value::min(),
        };
        let max = EAV {
            e: e.next(),
            a: Entity::min(),
            v: Value::min(),
        };
        self.eav.range(min..max).map(|eav| eav.clone().into())
    }

    fn iter_entity_attribute(&self, e: Entity, a: Entity) -> impl Iterator<Item = EAV> + '_ {
        let min = EAV {
            e,
            a: a.clone(),
            v: Value::min(),
        };
        let max = EAV {
            e,
            a: a.next(),
            v: Value::min(),
        };
        self.eav.range(min..max).map(|eav| eav.clone().into())
    }

    fn iter_attribute_value(&self, a: Entity, v: Value) -> impl Iterator<Item = EAV> + '_ {
        let min = AVE {
            a: a.clone(),
            v: v.clone(),
            e: Entity::min(),
        };
        let max = AVE {
            a: a.clone(),
            v: v.next(),
            e: Entity::min(),
        };
        self.ave.range(min..max).map(|ave| ave.clone().into())
    }

    fn resolve_pattern(&self, p: &Pattern<Attribute>) -> Result<Pattern<Entity>> {
        let a = match &p.a {
            Entry::Var(v) => Ok(Entry::Var(v.clone())),
            Entry::Lit(a) => {
                if let Some(id) = self.schema.get_id(&a) {
                    Ok(Entry::Lit(id))
                } else {
                    Err("Id not found".to_string())
                }
            }
        }?;

        Ok(Pattern {
            e: p.e.clone(),
            a,
            v: p.v.clone(),
        })
    }

    fn resolve_where(&self, whr: &Where<Attribute>) -> Result<Where<Entity>> {
        match whr {
            Where::Pattern(p) => Ok(Where::Pattern(self.resolve_pattern(&p)?)),
            Where::And(l, r) => Ok(Where::and(self.resolve_where(l)?, self.resolve_where(r)?)),
            Where::Or(l, r) => Ok(Where::or(self.resolve_where(l)?, self.resolve_where(r)?)),
        }
    }

    fn resolve_result(&self, table: HashSet<Vec<Value>>) -> HashSet<Vec<Value>> {
        table
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|val| {
                        if let Value::Int(i) = val {
                            if let Some(Attribute(a)) = self.schema.attributes.get(&Entity(i)) {
                                Value::Str(a.clone())
                            } else {
                                val
                            }
                        } else {
                            val
                        }
                    })
                    .collect()
            })
            .collect()
    }
}

#[derive(Clone, Copy)]
pub enum Type {
    Int,
    Ref,
    Float,
    Str,
}

pub enum Cardinality {
    One,
    Many,
}

pub struct AttributeDetails {
    type_: Type,
    cardinality: Cardinality,
}

pub struct Schema {
    details: HashMap<Attribute, AttributeDetails>,
    ids: HashMap<Attribute, Entity>,
    attributes: HashMap<Entity, Attribute>,
}

impl Schema {
    fn new() -> Self {
        Schema {
            details: HashMap::new(),
            ids: HashMap::new(),
            attributes: HashMap::new(),
        }
    }

    fn get_id(&self, a: &Attribute) -> Option<Entity> {
        self.ids.get(a).copied()
    }

    fn valid_type(&self, a: &Attribute, v: &Value) -> bool {
        if let Some(details) = self.details.get(a) {
            match (details.type_, v) {
                (Type::Int, Value::Int(_)) => true,
                (Type::Ref, Value::Int(_)) => true,
                (Type::Float, Value::Float(_)) => true,
                (Type::Str, Value::Str(_)) => true,
                _ => false,
            }
        } else {
            false
        }
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
    pub where_: Where<Attribute>,
}

impl Query {
    pub fn qeval(&self, store: &Store) -> Result<HashSet<Vec<Value>>> {
        let resolved_where = store.resolve_where(&self.where_)?;
        let frame_iter = resolved_where.qeval(store, vec![Frame::new()].into_iter());
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
pub enum Where<T> {
    Pattern(Pattern<T>),
    And(Box<Where<T>>, Box<Where<T>>),
    Or(Box<Where<T>>, Box<Where<T>>),
}

impl<T> Where<T> {
    pub fn and(l: Where<T>, r: Where<T>) -> Self {
        Self::And(Box::new(l), Box::new(r))
    }

    pub fn or(l: Where<T>, r: Where<T>) -> Self {
        Self::Or(Box::new(l), Box::new(r))
    }
}

impl Where<Entity> {
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
    Lazy(Box<dyn Iterator<Item = &'a EAV> + 'a>),
    Strict(Box<dyn Iterator<Item = EAV> + 'a>),
}

struct PatternI<'a, I> {
    pattern: &'a Pattern<Entity>,
    frames: I,
    current_frame: Option<Frame>,
    store: &'a Store,
    candidates: Candidates<'a>,
}

impl<'a, I: Iterator<Item = Frame>> PatternI<'a, I> {
    fn match_(&mut self, mut frame: Frame, datom: &EAV) -> Option<Frame> {
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
    left: &'a Box<Where<Entity>>,
    right: &'a Box<Where<Entity>>,
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

#[derive(Debug, PartialEq, Clone)]
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
    fn match_(&self, frame: &mut Frame, data: &T) -> Result<()> {
        match self {
            Entry::Lit(lit) => {
                if *lit == *data {
                    Ok(())
                } else {
                    Err("".to_string())
                }
            }
            Entry::Var(var) => {
                if let Some(bound_value) = frame.bound.get(&var) {
                    if data.compare_to_bound(bound_value) {
                        Ok(())
                    } else {
                        Err("".to_string())
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
pub struct Pattern<T> {
    pub e: Entry<Entity>,
    pub a: Entry<T>,
    pub v: Entry<Value>,
}

impl Pattern<Attribute> {
    fn match_(&self, frame: &mut Frame, datom: &Datom) -> Result<()> {
        self.e.match_(frame, &datom.e)?;
        self.a.match_(frame, &datom.a)?;
        self.v.match_(frame, &datom.v)?;
        Ok(())
    }
}

impl Pattern<Entity> {
    fn match_(&self, frame: &mut Frame, datom: &EAV) -> Result<()> {
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

    fn attribute_bound(&self, frame: &Frame) -> Option<Entity> {
        match self.a {
            Entry::Lit(ref a) => Some(a.clone()),
            Entry::Var(ref v) => {
                let val = frame.bound.get(v)?;
                match val {
                    Value::Int(a) => Some(Entity(*a)),
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

    fn entity_attribute_bound(&self, frame: &Frame) -> Option<(Entity, Entity)> {
        let e = self.entity_bound(frame)?;
        let a = self.attribute_bound(frame)?;
        Some((e, a))
    }

    fn attribute_value_bound(&self, frame: &Frame) -> Option<(Entity, Value)> {
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

    fn row(&self, vars: &[Var]) -> Result<Vec<Value>> {
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
        let mut store = Store::new();

        store
            .add_attribute("name", Type::Str, Cardinality::One)
            .unwrap();
        store
            .add_attribute("age", Type::Int, Cardinality::One)
            .unwrap();

        let datoms = vec![
            datom![100, :name "Moritz"],
            datom![100, :age 39],
            datom![150, :name "Moritz"],
            datom![150, :age 30],
            datom![200, :name "Piet"],
            datom![200, :age 39],
        ];

        for datom in datoms {
            store.insert(datom).unwrap();
        }

        store
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
            STORE.resolve_result(q.qeval(&STORE).unwrap()),
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
            STORE.resolve_result(q.qeval(&STORE).unwrap()),
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
