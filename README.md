## Todo

- [x] And/or queries
- [x] Indices
- [x] Persistence
- [ ] Rules
- [x] Pull API (and convert into Rust types)
  - [x] Pull Query Macro
- [ ] Cache queries
- [ ] Upserts
- [x] Schema
  - [ ] Component attributes
- [x] Enforce types
- [x] Attributes are entities under the hood
- [ ] Custom rust predicates
- [ ] Aggregation functions
- [ ] Enforce cardinality
- [ ] Transactions
- [ ] Transaction timestamps
- [x] Introspection (builtins)

---

add!(14, {
    "person/name": "Moritz",
    "person/age": 39,
    "person/friend": {
        "person/name": "Piet",
        "person/age": 3
    }
})
.and(retract!(14))

add!(14, "person/name": ["Moritz"]);

Compiles to

+ 14, person/name = Moritz
+ 14, person/age = 39
+ 14, person/friend = 15 (new)
+ 15, person/name = Piet
+ 15, person/age = 3

---

retract!(14, "person/name"),
retract!(14, "person/friend": Ref(15))
retract!(14, "person/aliases": ["Troll", "Papa"])

Compiles to

- 14, person/name = Moritz
- 14, person/friend = 15 (multiple if one-to-many attribute)
- 14, person/aliases = "Troll"
- 14, person/aliases = "Papa"
