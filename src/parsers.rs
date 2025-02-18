use nom::{
    branch::alt,
    bytes::complete::{tag, take_while, take_while1},
    character::complete::{char, digit1, multispace0, multispace1},
    combinator::{map, map_res, opt, peek, recognize},
    error::{ParseError, VerboseError},
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, preceded, separated_pair, tuple},
    IResult,
};

use crate::{
    pull::Api,
    query::{Entry, Pattern, Query, Var, Where},
    Attribute, Entity, Value,
};

type ParseResult<'a, T> = IResult<&'a str, T, VerboseError<&'a str>>;

/// Whitespace combinator
fn ws<'a, F: 'a, O, E: ParseError<&'a str>>(
    inner: F,
) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where
    F: FnMut(&'a str) -> IResult<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

pub fn parse_query(input: &str) -> ParseResult<Query> {
    delimited(
        ws(char('{')),
        map(
            separated_pair(parse_find, ws(char(',')), parse_where),
            |(find, where_)| Query { find, where_ },
        ),
        ws(char('}')),
    )(input)
}

fn parse_find(input: &str) -> ParseResult<Vec<Var>> {
    let (input, _) = ws(tag("find:"))(input)?;
    let (input, vars) = ws(delimited(
        char('['),
        separated_list0(ws(char(',')), parse_var_name),
        char(']'),
    ))(input)?;
    Ok((input, vars))
}

fn parse_where(input: &str) -> ParseResult<Where<Attribute>> {
    let (input, _) = ws(tag("where"))(input)?;
    let (input, _) = ws(char(':'))(input)?;
    let (input, items) = ws(delimited(
        char('['),
        separated_list0(ws(char(',')), parse_where_item),
        char(']'),
    ))(input)?;
    Ok((input, combine_where_items(items, "and")))
}

fn parse_where_item(input: &str) -> ParseResult<Where<Attribute>> {
    alt((
        parse_compound_where,
        map(parse_pattern, |p| Where::Pattern(p)),
    ))(input)
}

fn parse_compound_where(input: &str) -> ParseResult<Where<Attribute>> {
    let (input, op) = ws(alt((tag("or"), tag("and"))))(input)?;
    let (input, _) = ws(char(':'))(input)?;
    let (input, subitems) = ws(delimited(
        char('['),
        separated_list0(ws(char(',')), parse_where_item),
        char(']'),
    ))(input)?;
    Ok((input, combine_where_items(subitems, op)))
}

fn combine_where_items(items: Vec<Where<Attribute>>, op: &str) -> Where<Attribute> {
    let mut iter = items.into_iter();
    let first = iter.next().expect("at least one clause in compound");
    match op {
        "and" => iter.fold(first, |acc, item| Where::And(Box::new(acc), Box::new(item))),
        "or" => iter.fold(first, |acc, item| Where::Or(Box::new(acc), Box::new(item))),
        _ => unreachable!(),
    }
}

fn parse_pattern(input: &str) -> ParseResult<Pattern<Attribute>> {
    let (input, _) = ws(char('('))(input)?;
    let (input, e) = ws(parse_entry_entity)(input)?;
    let (input, _) = ws(char(','))(input)?;
    let (input, a) = ws(parse_entry_attribute)(input)?;
    let (input, _) = ws(char('='))(input)?;
    let (input, v) = ws(parse_entry_value)(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((input, Pattern { e, a, v }))
}

fn parse_entry_entity(input: &str) -> ParseResult<Entry<Entity>> {
    alt((
        map(parse_var_name, Entry::Var),
        map(parse_entity, Entry::Lit),
    ))(input)
}

fn parse_entry_attribute(input: &str) -> ParseResult<Entry<Attribute>> {
    alt((
        map(parse_var_name, Entry::Var),
        map(parse_attribute, |a| Entry::Lit(a)),
    ))(input)
}

fn parse_entry_value(input: &str) -> ParseResult<Entry<Value>> {
    alt((
        map(parse_var_name, Entry::Var),
        map(parse_value, Entry::Lit),
    ))(input)
}

fn parse_var_name(input: &str) -> ParseResult<Var> {
    let (input, _) = char('?')(input)?;
    let (input, ident) = take_while(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    Ok((input, Var(ident.to_string())))
}

fn parse_entity(input: &str) -> ParseResult<Entity> {
    map(parse_u64, |id| Entity(id))(input)
}

fn parse_u64(input: &str) -> ParseResult<u64> {
    map_res(ws(digit1), |s: &str| s.parse::<u64>())(input)
}

fn parse_string(input: &str) -> ParseResult<String> {
    delimited(
        char('"'),
        map(take_while(|c| c != '"'), |s: &str| s.to_string()),
        char('"'),
    )(input)
}

fn parse_float(input: &str) -> ParseResult<f64> {
    map_res(recognize(tuple((digit1, char('.'), digit1))), |s: &str| {
        s.parse::<f64>()
    })(input)
}

fn parse_value(input: &str) -> ParseResult<Value> {
    alt((
        // String literal
        map(parse_string, Value::Str),
        // Bool literal
        map(alt((tag("true"), tag("false"))), |s: &str| {
            Value::Bool(s == "true")
        }),
        // Float literal
        map(parse_float, |f| (f as f64).into()),
        // Integer literal
        map(parse_u64, Value::Int),
        // Ref literal
        map(delimited(tag("Ref("), parse_u64, tag(")")), Value::Ref),
    ))(input)
}

/// Parse an item: it may be preceded by `<-` (for a `Back`), then a string,
/// and optionally a colon and a nested list.
/// A bare string is taken as an `In` with `Return` as its nested value.
fn parse_pull_item(input: &str) -> ParseResult<Api> {
    // Optional `<-` indicates a backward traversal.
    let (input, maybe_back) = opt(ws(tag("<-")))(input)?;
    // Parse the field name.
    let (input, field) = parse_string(input)?;
    // Optionally, parse a colon and a nested block.
    let (input, nested) = opt(preceded(ws(char(':')), parse_pull_list))(input)?;

    let attribute = Attribute(field);

    let api = match (maybe_back, nested) {
        (Some(_), Some(n)) => Api::Back(attribute, Box::new(n)),
        (Some(_), None) => Api::Back(attribute, Box::new(Api::Return)),
        (None, Some(n)) => Api::In(attribute, Box::new(n)),
        (None, None) => Api::In(attribute, Box::new(Api::Return)),
    };
    Ok((input, api))
}

/// Parse a list of items, i.e. a block delimited by `{` and `}`.
fn parse_pull_list(input: &str) -> ParseResult<Api> {
    let (input, items) = delimited(
        ws(char('{')),
        separated_list0(ws(char(',')), parse_pull_item),
        ws(char('}')),
    )(input)?;
    Ok((input, Api::List(items)))
}

/// Top-level parser.
pub fn parse_pull(input: &str) -> ParseResult<Api> {
    alt((parse_pull_list, parse_pull_item))(input)
}

/// add Tmp(a) {
///   "name": "Moritz",
///   "age": 39
/// }
///
/// add 1 {
///   "friend": [
///     "age": 39
///   ]
/// }
///
/// add {
///   "name": "Moritz"
/// }
///
/// retract Tmp(a)
///
/// retract 14 "name" "Moritz"
///
use crate::transact as tx;

fn parse_tmpref(input: &str) -> ParseResult<String> {
    let (input, _) = tag("Tmp(")(input)?;
    let (input, str) = take_while1(|c: char| c.is_alphabetic() || c == '_')(input)?;
    let (input, _) = tag(")")(input)?;
    Ok((input, str.to_string()))
}

// Parse entity (number or temp reference)
fn parse_tx_entity(input: &str) -> ParseResult<tx::Entity> {
    alt((
        map(parse_entity, |e| tx::Entity::Entity(e)),
        map(parse_tmpref, |t| tx::Entity::TempRef(t)),
    ))(input)
}

fn parse_tx_value(input: &str) -> ParseResult<tx::Value> {
    alt((
        map(parse_value, |v| tx::Value::Value(v)),
        map(parse_tmpref, |t| tx::Value::TempRef(t)),
    ))(input)
}

// Parse attribute
fn parse_attribute(input: &str) -> ParseResult<Attribute> {
    map(parse_string, Attribute)(input)
}

// Parse a single key-value pair (value = value / list / component)
fn parse_add_key_value(input: &str) -> ParseResult<tx::Add> {
    let (input, a) = ws(parse_attribute)(input)?;
    let (input, _) = char(':')(input)?;
    let (input, result) = alt((
        map(ws(parse_tx_value), |v| tx::Add::Value { a: a.clone(), v }),
        map(ws(parse_add_block), |c| tx::Add::Component {
            a: a.clone(),
            sub: Box::new(c),
        }),
        map(ws(parse_add_list), |l| {
            tx::Add::List(
                l.into_iter()
                    .map(|vc| match vc {
                        ValueOrComponent::Component(c) => tx::Add::Component {
                            a: a.clone(),
                            sub: Box::new(c),
                        },
                        ValueOrComponent::Value(v) => tx::Add::Value { a: a.clone(), v },
                    })
                    .collect(),
            )
        }),
    ))(input)?;
    Ok((input, result))
}

// Parse add block contents
fn parse_add_block(input: &str) -> ParseResult<tx::Add> {
    let (input, items) = delimited(
        ws(char('{')),
        separated_list1(ws(char(',')), parse_add_key_value),
        ws(char('}')),
    )(input)?;
    Ok((input, tx::Add::List(items)))
}

enum ValueOrComponent {
    Value(tx::Value),
    Component(tx::Add),
}

// list of values/components
fn parse_add_list(input: &str) -> ParseResult<Vec<ValueOrComponent>> {
    delimited(
        ws(char('[')),
        separated_list1(
            ws(char(',')),
            alt((
                map(parse_tx_value, |v| ValueOrComponent::Value(v)),
                map(parse_add_block, |c| ValueOrComponent::Component(c)),
            )),
        ),
        ws(char(']')),
    )(input)
}

// Parse add transaction
fn parse_add(input: &str) -> ParseResult<tx::Transact> {
    let (input, (e, add)) = tuple((
        preceded(ws(tag("add")), opt(ws(parse_tx_entity))),
        parse_add_block,
    ))(input)?;
    let e = e.unwrap_or(tx::Entity::TempRef("x".to_string()));
    Ok((input, tx::Transact::Add { e, add }))
}

// Parse retract transaction
fn parse_retract(input: &str) -> ParseResult<tx::Transact> {
    let (input, e) = preceded(tag("retract"), preceded(multispace1, parse_tx_entity))(input)?;
    let (input, attr_val) = opt(preceded(
        multispace1,
        tuple((parse_attribute, opt(preceded(multispace1, parse_tx_value)))),
    ))(input)?;

    Ok((
        input,
        match attr_val {
            Some((a, Some(v))) => tx::Transact::RetractValue { e, a, v },
            Some((a, None)) => tx::Transact::RetractAttribute { e, a },
            None => tx::Transact::Retract { e },
        },
    ))
}

// Parse a single transaction
pub fn parse_transaction(input: &str) -> ParseResult<tx::Transact> {
    let (input, keyword) = ws(peek(take_while(|c: char| c.is_alphabetic())))(input)?;
    match keyword {
        "add" => parse_add(input),
        "retract" => parse_retract(input),
        _ => Err(nom::Err::Error(VerboseError::from_char(input, 'a'))),
    }
}

// Parse multiple transactions
pub fn parse_transactions(input: &str) -> ParseResult<tx::Transact> {
    let (input, trans) = many0(ws(parse_transaction))(input)?;
    Ok((input, tx::Transact::List(trans)))
}

#[cfg(test)]
mod tests {
    use nom::{error::convert_error, Finish};
    use pretty_assertions::assert_eq;

    use crate::{add, parsers::parse_transaction, pull, query, retract, transact as tx, Value};

    use super::{parse_pull, parse_query, ParseResult};

    fn run<F, T>(mut f: F, input: &str) -> T
    where
        F: FnMut(&str) -> ParseResult<T>,
    {
        match f(input).finish() {
            Ok((_, t)) => t,
            Err(e) => panic!("{}", convert_error(input, e)),
        }
    }

    #[test]
    fn test_query() {
        let input = r#"{ find: [?a], where: [(?a, "foo" = Ref(3))] }"#;
        let (_, query) = parse_query(input).unwrap();

        let expected = query! {
            find: [?a],
            where: [
                (?a, "foo" = Value::Ref(3))
            ]
        };

        assert_eq!(query, expected);
    }

    #[test]
    fn test_query2() {
        let input = r#"
            {
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
            }
        "#;
        let (_, query) = parse_query(input).unwrap();

        let expected = query! {
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

        assert_eq!(query, expected);
    }

    #[test]
    fn test_pull() {
        let input = r#"
            {
                "movie/title",
                "movie/cast": {
                    "person/name",
                    <- "movie/cast": {
                        "movie/title"
                    }
                }
            }
        "#;
        let (_, query) = parse_pull(input).unwrap();

        let expected = pull!({
            "movie/title",
            "movie/cast": {
                "person/name",
                <- "movie/cast": {
                    "movie/title"
                }
            }
        });

        assert_eq!(query, expected);
    }

    #[test]
    fn test_transact1() {
        let input = r#"
            add Tmp(a) {
              "name": "Moritz",
              "age": 39
            }
        "#;

        let expected = add!(tx::Tmp("a"), {
           "name": "Moritz",
           "age": 39
        });

        assert_eq!(run(parse_transaction, input), expected);
    }

    #[test]
    fn test_transact2() {
        let input = r#"
            add 1 {
              "friend": [{
                "age": 39
              }]
            }
        "#;

        let expected = add!(1, {
           "friend": [{
               "age": 39
           }]
        });

        assert_eq!(run(parse_transaction, input), expected);
    }

    #[test]
    fn test_transact3() {
        let input = r#"
          add {
            "name": "Moritz"
          }
        "#;

        let expected = add!({
           "name": "Moritz"
        });

        assert_eq!(run(parse_transaction, input), expected);
    }

    #[test]
    fn test_transact4() {
        let input = r#"retract Tmp(a)"#;
        let expected = retract!(tx::Tmp("a"));
        assert_eq!(run(parse_transaction, input), expected);
    }

    #[test]
    fn test_transact5() {
        let input = r#"retract 14 "name" "Moritz""#;
        let expected = retract!(14, "name": "Moritz");
        assert_eq!(run(parse_transaction, input), expected);
    }
}
