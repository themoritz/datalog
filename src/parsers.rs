use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::{char, digit1, multispace0},
    combinator::{map, map_res, recognize},
    error::ParseError,
    multi::separated_list0,
    sequence::{delimited, separated_pair, tuple},
    IResult,
};

use crate::{
    query::{Entry, Pattern, Query, Var, Where},
    Attribute, Entity, Value,
};

/// Whitespace combinator
fn ws<'a, F: 'a, O, E: ParseError<&'a str>>(
    inner: F,
) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where
    F: FnMut(&'a str) -> IResult<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

pub fn parse_query(input: &str) -> IResult<&str, Query> {
    delimited(
        ws(char('{')),
        map(
            separated_pair(parse_find, ws(char(',')), parse_where),
            |(find, where_)| Query { find, where_ },
        ),
        ws(char('}')),
    )(input)
}

fn parse_find(input: &str) -> IResult<&str, Vec<Var>> {
    let (input, _) = ws(tag("find:"))(input)?;
    let (input, vars) = ws(delimited(
        char('['),
        separated_list0(ws(char(',')), parse_var_name),
        char(']'),
    ))(input)?;
    Ok((input, vars))
}

fn parse_where(input: &str) -> IResult<&str, Where<Attribute>> {
    let (input, _) = ws(tag("where"))(input)?;
    let (input, _) = ws(char(':'))(input)?;
    let (input, items) = ws(delimited(
        char('['),
        separated_list0(ws(char(',')), parse_where_item),
        char(']'),
    ))(input)?;
    Ok((input, combine_where_items(items, "and")))
}

fn parse_where_item(input: &str) -> IResult<&str, Where<Attribute>> {
    alt((
        parse_compound_where,
        map(parse_pattern, |p| Where::Pattern(p)),
    ))(input)
}

fn parse_compound_where(input: &str) -> IResult<&str, Where<Attribute>> {
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

fn parse_pattern(input: &str) -> IResult<&str, Pattern<Attribute>> {
    let (input, _) = ws(char('('))(input)?;
    let (input, e) = ws(parse_entry_entity)(input)?;
    let (input, _) = ws(char(','))(input)?;
    let (input, a) = ws(parse_entry_attribute)(input)?;
    let (input, _) = ws(char('='))(input)?;
    let (input, v) = ws(parse_entry_value)(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((input, Pattern { e, a, v }))
}

fn parse_entry_entity(input: &str) -> IResult<&str, Entry<Entity>> {
    alt((
        map(parse_var_name, Entry::Var),
        map(parse_entity, Entry::Lit),
    ))(input)
}

fn parse_entry_attribute(input: &str) -> IResult<&str, Entry<Attribute>> {
    alt((
        map(parse_var_name, Entry::Var),
        map(parse_string_literal, |s| Entry::Lit(Attribute(s))),
    ))(input)
}

fn parse_entry_value(input: &str) -> IResult<&str, Entry<Value>> {
    alt((
        map(parse_var_name, Entry::Var),
        map(parse_value, Entry::Lit),
    ))(input)
}

fn parse_var_name(input: &str) -> IResult<&str, Var> {
    let (input, _) = char('?')(input)?;
    let (input, ident) = take_while(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    Ok((input, Var(ident.to_string())))
}

fn parse_entity(input: &str) -> IResult<&str, Entity> {
    map(parse_u64, |id| Entity(id))(input)
}

fn parse_u64(input: &str) -> IResult<&str, u64> {
    map_res(ws(digit1), |s: &str| s.parse::<u64>())(input)
}

fn parse_string_literal(input: &str) -> IResult<&str, String> {
    delimited(
        char('"'),
        map(take_while(|c| c != '"'), |s: &str| s.to_string()),
        char('"'),
    )(input)
}

fn parse_float(input: &str) -> IResult<&str, f64> {
    map_res(recognize(tuple((digit1, char('.'), digit1))), |s: &str| {
        s.parse::<f64>()
    })(input)
}

fn parse_value(input: &str) -> IResult<&str, Value> {
    alt((
        // String literal
        map(parse_string_literal, Value::Str),
        // Bool literal
        map(alt((tag("true"), tag("false"))), |s: &str| {
            Value::Bool(s == "true")
        }),
        // Float literal
        map(parse_float, |f| (f as f64).into()),
        // Integer literal
        map(parse_u64, Value::Int),
        // Ref literal
        map(delimited(tag("Ref("), parse_u64, tag(")")), Value::Ref)
    ))(input)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::{query, Value};

    use super::parse_query;

    #[test]
    fn test_parse() {
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
    fn test_parse2() {
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
}
