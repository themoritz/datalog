use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
};

use crate::{
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    use crate::{datom, movies::DATA, movies::STORE, row, table};

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

        assert_eq!(q.qeval(&DATA).unwrap(), table![[100, "Moritz"]]);
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

        assert_eq!(q.qeval(&DATA).unwrap(), table![[200], [100]]);
    }

    #[test]
    fn qeval_builtins() {
        let q = query! {
            find: [?doc],
            where: [
                [?e, :db/ident "name"]
                [?e, :db/doc ?doc]
            ]
        };

        assert_eq!(q.qeval(&DATA).unwrap(), table![["The name"]]);

        let q = query! {
            find: [?e],
            where: [
                [?e, :db/ident "db/ident"]
            ]
        };

        assert_eq!(q.qeval(&DATA).unwrap(), table![[0]]);
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
