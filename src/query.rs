use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
};

use crate::{
    mem_store::MemStore,
    store::{Store, EAV},
    Attribute, Data, Datom, Entity, Result, Value,
};

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
    pub fn qeval(&self, store: &impl Store) -> Result<HashSet<Vec<Value>>> {
        let resolved_where = store.resolve_where(&self.where_)?;
        let frame_iter = resolved_where.qeval(store, vec![Frame::new()].into_iter());
        frame_iter.map(|frame| frame.row(&self.find)).collect()
    }

    fn print_result(&self, store: &MemStore) {
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
    fn qeval<'a, S: Store>(
        &'a self,
        store: &'a S,
        frames: impl Iterator<Item = Frame> + 'a,
    ) -> Box<dyn Iterator<Item = Frame> + 'a> {
        match self {
            Where::Pattern(pattern) => Box::new(PatternI {
                pattern,
                frames,
                current_frame: None,
                store,
                candidates: Box::new(Vec::new().into_iter()),
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

struct PatternI<'a, I, S> {
    pattern: &'a Pattern<Entity>,
    frames: I,
    current_frame: Option<Frame>,
    store: &'a S,
    candidates: Box<dyn Iterator<Item = EAV> + 'a>,
}

impl<'a, I: Iterator<Item = Frame>, S: Store> PatternI<'a, I, S> {
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
                    self.candidates = Box::new(self.store.iter_entity_attribute(entity, attribute));
                } else if let Some(entity) = self.pattern.entity_bound(&frame) {
                    self.candidates = Box::new(self.store.iter_entity(entity));
                } else if let Some((attribute, value)) = self.pattern.attribute_value_bound(&frame)
                {
                    self.candidates = Box::new(self.store.iter_attribute_value(attribute, value));
                } else {
                    self.candidates = Box::new(self.store.iter());
                }
                self.next()
            }
            None => None,
        }
    }
}

impl<'a, I: Iterator<Item = Frame>, S: Store> Iterator for PatternI<'a, I, S> {
    type Item = Frame;

    // TODO: Avoid recursion
    fn next(&mut self) -> Option<Self::Item> {
        match self.current_frame {
            Some(ref frame) => match self.candidates.next() {
                Some(datom) => self.match_(frame.clone(), &datom),
                None => self.next_frame(),
            },
            None => self.next_frame(),
        }
    }
}

struct OrI<'a, I, S> {
    inner: I,
    store: &'a S,
    left: &'a Box<Where<Entity>>,
    right: &'a Box<Where<Entity>>,
    queue: VecDeque<Frame>,
}

impl<'a, I: Iterator<Item = Frame>, S: Store> Iterator for OrI<'a, I, S> {
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
                    Value::Ref(e) => Some(Entity(*e)),
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
    (find: [ $(?$var:ident),* ], where: [ $($tt:tt)* ]) => {
        $crate::query::Query {
            find: vec![$(query!(@var ?$var)),*],
            where_: query!(@where (and) [] $($tt)*)
        }
    };

    // VAR //

    (@var ?$var:ident) => {
        $crate::query::Var(stringify!($var).to_string())
    };

    // ENTRY //

    (@entry ?$var:ident) => {
        $crate::query::Entry::Var(query!(@var ?$var))
    };

    (@entry $val:expr) => {
        $crate::query::Entry::Lit($crate::Value::from($val))
    };

    (@entry_entity $val:expr) => {
        $crate::query::Entry::Lit(Entity($val))
    };

    (@entry_attr $var:literal) => {
        $crate::query::Entry::Lit($crate::Attribute($var.to_string()))
    };

    // PATTERN //

    // Done
    (@pattern ($e:expr) ($a:expr) ($v:expr)) => {
        $crate::query::Where::Pattern($crate::query::Pattern {
            e: $e,
            a: $a,
            v: $v
        })
    };

    // Fill entity for vars
    (@pattern () () () ?$var:ident, $($rest:tt)+) => {
        query!(@pattern (query!(@entry ?$var)) () () $($rest)+)
    };

    // Fill entity for vals
    (@pattern () () () $val:expr, $($rest:tt)+) => {
        query!(@pattern (query!(@entry_entity $val)) () () $($rest)+)
    };

    // Fill attribute and value for var attributes
    (@pattern ($e:expr) () () ?$var:ident = $($rest:tt)+) => {
        query!(@pattern ($e) (query!(@entry ?$var)) (query!(@entry $($rest)+)))
    };

    // Fill attribute and value for val attributes
    (@pattern ($e:expr) () () $val:literal = $($rest:tt)+) => {
        query!(@pattern ($e) (query!(@entry_attr $val)) (query!(@entry $($rest)+)))
    };

    // WHERE //

    (@where ($cons:ident) [$($elems:expr,)*]) => {{
        let mut iter = vec![$($elems),*].into_iter();
        let first = iter.next().unwrap();
        iter.fold(first, |acc, e| $crate::query::Where::$cons(acc, e))
    }};

    // Next pattern
    (@where ($cons:ident) [$($elems:expr,)*] ($($tt:tt)+) $($rest:tt)*) => {
        query!(@where ($cons) [$($elems,)* query!(@pattern () () () $($tt)+),] $($rest)*)
    };

    // Next or
    (@where ($cons:ident) [$($elems:expr,)*] or: [ $($tt:tt)* ] $($rest:tt)*) => {
        query!(@where ($cons) [$($elems,)* query!(@where (or) [] $($tt)*),] $($rest)*)
    };

    // Next and
    (@where ($cons:ident) [$($elems:expr,)*] and: [ $($tt:tt)* ] $($rest:tt)*) => {
        query!(@where ($cons) [$($elems,)* query!(@where (and) [] $($tt)*),] $($rest)*)
    };

    // Comma
    (@where ($cons:ident) [$($elems:expr,)*], $($rest:tt)*) => {
        query!(@where ($cons) [$($elems,)*] $($rest)*)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    use crate::{
        datom,
        mem_store::MemStore,
        movies::{DATA, STORE},
        row, table, Ref,
    };

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
                (3, "foo" = 3),
                (1, "bar" = ?a),
                (0, ?b = "M"),
                (0, ?b = ?a),
                or: [
                    (?x, "foo" = 3),
                    (?x, "bar" = ?a)
                ],
                and: [
                    (?x, ?b = "M"),
                    (?x, ?b = ?a)
                ]
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
            .match_(&mut frame, &datom!(100, "name" = "Moritz"))
            .unwrap();
        let row = frame.row(&[Var("a".to_string())]).unwrap();
        assert_eq!(row, vec![Value::Ref(100)]);

        assert!(pattern
            .match_(&mut frame, &datom!(100, "name" = 1))
            .is_err());
        assert!(pattern
            .match_(&mut frame, &datom!(100, "name" = "Moritz"))
            .is_ok());
        assert!(pattern
            .match_(&mut frame, &datom!(200, "name" = "Moritz"))
            .is_err());
    }

    #[test]
    fn qeval_pattern() {
        let q = query! {
            find: [?p, ?name],
            where: [
                (?p, "name" = "Moritz"),
                (?p, "name" = ?name),
                (?p, "age" = 39)
            ]
        };

        let data: &MemStore = &DATA;
        assert_eq!(q.qeval(data).unwrap(), table![[Ref(100), "Moritz"]]);
    }

    #[test]
    fn qeval_or() {
        let q = query! {
            find: [?e],
            where: [
                or: [
                    (?e, "name" = "Piet"),
                    (?e, "name" = "Moritz")
                ],
                (?e, "age" = 39)
            ]
        };

        let data: &MemStore = &DATA;
        assert_eq!(q.qeval(data).unwrap(), table![[Ref(200)], [Ref(100)]]);
    }

    #[test]
    fn qeval_builtins() {
        let q = query! {
            find: [?doc],
            where: [
                (?e, "db/ident" = "name"),
                (?e, "db/doc" = ?doc)
            ]
        };

        let data: &MemStore = &DATA;
        assert_eq!(q.qeval(data).unwrap(), table![["The name"]]);

        let q = query! {
            find: [?e],
            where: [
                (?e, "db/ident" = "db/ident")
            ]
        };

        let data: &MemStore = &DATA;
        assert_eq!(q.qeval(data).unwrap(), table![[Ref(0)]]);
    }

    #[test]
    fn movies_alien() {
        let q = query! {
            find: [?year],
            where: [
                (?id, "movie/title" = "Alien"),
                (?id, "movie/year" = ?year)
            ]
        };

        let store: &MemStore = &STORE;
        assert_eq!(q.qeval(store).unwrap(), table![[1979]]);
    }

    #[test]
    fn movies_200() {
        let q = query! {
            find: [?attr, ?value],
            where: [
                (200, ?attr = ?value)
            ]
        };

        let store: &MemStore = &STORE;
        assert_eq!(
            STORE.resolve_result(q.qeval(store).unwrap()),
            table![
                ["movie/title", "The Terminator"],
                ["movie/year", 1984],
                ["movie/director", Ref(100)],
                ["movie/cast", Ref(101)],
                ["movie/cast", Ref(102)],
                ["movie/cast", Ref(103)],
                ["movie/sequel", Ref(207)]
            ]
        );
    }

    #[test]
    fn movies_arnold() {
        let q = query! {
            find: [?director, ?movie],
            where: [
                (?a, "person/name" = "Arnold Schwarzenegger"),
                (?m, "movie/cast" = ?a),
                (?m, "movie/title" = ?movie),
                (?m, "movie/director" = ?d),
                (?d, "person/name" = ?director)
            ]
        };

        let store: &MemStore = &STORE;
        let result = q.qeval(store).unwrap();
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
                (?m, "movie/title" = ?title),
                (?m, "movie/year" = 1985)
            ]
        };

        let store: &MemStore = &STORE;
        assert_eq!(
            q.qeval(store).unwrap(),
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
                (?m, "movie/title" = "Commando"),
                (?m, ?attr = ?v)
            ]
        };

        let store: &MemStore = &STORE;
        assert_eq!(
            STORE.resolve_result(q.qeval(store).unwrap()),
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
