## Todo

- [x] And/or queries
- [x] Indices
- [ ] Persistence
- [ ] Rules
- [ ] Pull API (and convert into Rust types)
- [ ] Cache queries
- [ ] Upserts
- [x] Schema + enforce types
- [x] Attributes are entities under the hood
- [ ] Custom rust predicates
- [ ] Aggregation functions
- [ ] Enforce cardinality
- [ ] Transaction timestamps


built-in:

1: { ident: "db/ident", type: str, card: one }
2: { ident: "db/type", type: enum, card: one }
3: { ident: "db/cardinality", type: enum, card: one }
4: { ident: "db/doc", type: str, card: one }

100 1 "name"
100 2 "str"
100 3 "one"
100 4 "the name of the person"

1000 100 "Moritz"
2000 100 "Hannes"
