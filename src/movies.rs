use lazy_static;

use crate::{Store, Datom, Entity, Attribute, Value};

// TODO: Export?
macro_rules! datom {
    [ $e:expr, :$a:ident$(/$b:ident)* $v:expr ] => {
        Datom {
            e: Entity($e),
            a: Attribute(concat!(stringify!($a) $(, "/", stringify!($b) )* ).to_string()),
            v: Value::from($v),
        }
    };
}

lazy_static::lazy_static! {
    static ref STORE: Store = Store { data: vec![
        datom![100, :person/name "James Cameron"],
        datom![100, :person/born "1954-08-16T00:00:00Z"],
        datom![101, :person/name "Arnold Schwarzenegger"],
        datom![101, :person/born "1947-07-30T00:00:00Z"],
        datom![102, :person/name "Linda Hamilton"],
        datom![102, :person/born "1956-09-26T00:00:00Z"],
        datom![103, :person/name "Michael Biehn"],
        datom![103, :person/born "1956-07-31T00:00:00Z"],
        datom![104, :person/name "Ted Kotcheff"],
        datom![104, :person/born "1931-04-07T00:00:00Z"],
        datom![105, :person/name "Sylvester Stallone"],
        datom![105, :person/born "1946-07-06T00:00:00Z"],
        datom![106, :person/name "Richard Crenna"],
        datom![106, :person/born "1926-11-30T00:00:00Z"],
        datom![106, :person/death "2003-01-17T00:00:00Z"],
        datom![107, :person/name "Brian Dennehy"],
        datom![107, :person/born "1938-07-09T00:00:00Z"],
        datom![108, :person/name "John McTiernan"],
        datom![108, :person/born "1951-01-08T00:00:00Z"],
        datom![109, :person/name "Elpidia Carrillo"],
        datom![109, :person/born "1961-08-16T00:00:00Z"],
        datom![110, :person/name "Carl Weathers"],
        datom![110, :person/born "1948-01-14T00:00:00Z"],
        datom![111, :person/name "Richard Donner"],
        datom![111, :person/born "1930-04-24T00:00:00Z"],
        datom![112, :person/name "Mel Gibson"],
        datom![112, :person/born "1956-01-03T00:00:00Z"],
        datom![113, :person/name "Danny Glover"],
        datom![113, :person/born "1946-07-22T00:00:00Z"],
        datom![114, :person/name "Gary Busey"],
        datom![114, :person/born "1944-07-29T00:00:00Z"],
        datom![115, :person/name "Paul Verhoeven"],
        datom![115, :person/born "1938-07-18T00:00:00Z"],
        datom![116, :person/name "Peter Weller"],
        datom![116, :person/born "1947-06-24T00:00:00Z"],
        datom![117, :person/name "Nancy Allen"],
        datom![117, :person/born "1950-06-24T00:00:00Z"],
        datom![118, :person/name "Ronny Cox"],
        datom![118, :person/born "1938-07-23T00:00:00Z"],
        datom![119, :person/name "Mark L. Lester"],
        datom![119, :person/born "1946-11-26T00:00:00Z"],
        datom![120, :person/name "Rae Dawn Chong"],
        datom![120, :person/born "1961-02-28T00:00:00Z"],
        datom![121, :person/name "Alyssa Milano"],
        datom![121, :person/born "1972-12-19T00:00:00Z"],
        datom![122, :person/name "Bruce Willis"],
        datom![122, :person/born "1955-03-19T00:00:00Z"],
        datom![123, :person/name "Alan Rickman"],
        datom![123, :person/born "1946-02-21T00:00:00Z"],
        datom![124, :person/name "Alexander Godunov"],
        datom![124, :person/born "1949-11-28T00:00:00Z"],
        datom![124, :person/death "1995-05-18T00:00:00Z"],
        datom![125, :person/name "Robert Patrick"],
        datom![125, :person/born "1958-11-05T00:00:00Z"],
        datom![126, :person/name "Edward Furlong"],
        datom![126, :person/born "1977-08-02T00:00:00Z"],
        datom![127, :person/name "Jonathan Mostow"],
        datom![127, :person/born "1961-11-28T00:00:00Z"],
        datom![128, :person/name "Nick Stahl"],
        datom![128, :person/born "1979-12-05T00:00:00Z"],
        datom![129, :person/name "Claire Danes"],
        datom![129, :person/born "1979-04-12T00:00:00Z"],
        datom![130, :person/name "George P. Cosmatos"],
        datom![130, :person/born "1941-01-04T00:00:00Z"],
        datom![130, :person/death "2005-04-19T00:00:00Z"],
        datom![131, :person/name "Charles Napier"],
        datom![131, :person/born "1936-04-12T00:00:00Z"],
        datom![131, :person/death "2011-10-05T00:00:00Z"],
        datom![132, :person/name "Peter MacDonald"],
        datom![133, :person/name "Marc de Jonge"],
        datom![133, :person/born "1949-02-16T00:00:00Z"],
        datom![133, :person/death "1996-06-06T00:00:00Z"],
        datom![134, :person/name "Stephen Hopkins"],
        datom![135, :person/name "Ruben Blades"],
        datom![135, :person/born "1948-07-16T00:00:00Z"],
        datom![136, :person/name "Joe Pesci"],
        datom![136, :person/born "1943-02-09T00:00:00Z"],
        datom![137, :person/name "Ridley Scott"],
        datom![137, :person/born "1937-11-30T00:00:00Z"],
        datom![138, :person/name "Tom Skerritt"],
        datom![138, :person/born "1933-08-25T00:00:00Z"],
        datom![139, :person/name "Sigourney Weaver"],
        datom![139, :person/born "1949-10-08T00:00:00Z"],
        datom![140, :person/name "Veronica Cartwright"],
        datom![140, :person/born "1949-04-20T00:00:00Z"],
        datom![141, :person/name "Carrie Henn"],
        datom![142, :person/name "George Miller"],
        datom![142, :person/born "1945-03-03T00:00:00Z"],
        datom![143, :person/name "Steve Bisley"],
        datom![143, :person/born "1951-12-26T00:00:00Z"],
        datom![144, :person/name "Joanne Samuel"],
        datom![145, :person/name "Michael Preston"],
        datom![145, :person/born "1938-05-14T00:00:00Z"],
        datom![146, :person/name "Bruce Spence"],
        datom![146, :person/born "1945-09-17T00:00:00Z"],
        datom![147, :person/name "George Ogilvie"],
        datom![147, :person/born "1931-03-05T00:00:00Z"],
        datom![148, :person/name "Tina Turner"],
        datom![148, :person/born "1939-11-26T00:00:00Z"],
        datom![149, :person/name "Sophie Marceau"],
        datom![149, :person/born "1966-11-17T00:00:00Z"],
        datom![200, :movie/title "The Terminator"],
        datom![200, :movie/year 1984],
        datom![200, :movie/director 100],
        datom![200, :movie/cast 101],
        datom![200, :movie/cast 102],
        datom![200, :movie/cast 103],
        datom![200, :movie/sequel 207],
        datom![201, :movie/title "First Blood"],
        datom![201, :movie/year 1982],
        datom![201, :movie/director 104],
        datom![201, :movie/cast 105],
        datom![201, :movie/cast 106],
        datom![201, :movie/cast 107],
        datom![201, :movie/sequel 209],
        datom![202, :movie/title "Predator"],
        datom![202, :movie/year 1987],
        datom![202, :movie/director 108],
        datom![202, :movie/cast 101],
        datom![202, :movie/cast 109],
        datom![202, :movie/cast 110],
        datom![202, :movie/sequel 211],
        datom![203, :movie/title "Lethal Weapon"],
        datom![203, :movie/year 1987],
        datom![203, :movie/director 111],
        datom![203, :movie/cast 112],
        datom![203, :movie/cast 113],
        datom![203, :movie/cast 114],
        datom![203, :movie/sequel 212],
        datom![204, :movie/title "RoboCop"],
        datom![204, :movie/year 1987],
        datom![204, :movie/director 115],
        datom![204, :movie/cast 116],
        datom![204, :movie/cast 117],
        datom![204, :movie/cast 118],
        datom![205, :movie/title "Commando"],
        datom![205, :movie/year 1985],
        datom![205, :movie/director 119],
        datom![205, :movie/cast 101],
        datom![205, :movie/cast 120],
        datom![205, :movie/cast 121],
        datom![
            205,
            :trivia
            "In 1986, a sequel was written with an eye to having\n  John McTiernan direct. Schwarzenegger wasn't interested in reprising\n  the role. The script was then reworked with a new central character,\n  eventually played by Bruce Willis, and became Die Hard"
        ],
        datom![206, :movie/title "Die Hard"],
        datom![206, :movie/year 1988],
        datom![206, :movie/director 108],
        datom![206, :movie/cast 122],
        datom![206, :movie/cast 123],
        datom![206, :movie/cast 124],
        datom![207, :movie/title "Terminator 2: Judgment Day"],
        datom![207, :movie/year 1991],
        datom![207, :movie/director 100],
        datom![207, :movie/cast 101],
        datom![207, :movie/cast 102],
        datom![207, :movie/cast 125],
        datom![207, :movie/cast 126],
        datom![207, :movie/sequel 208],
        datom![208, :movie/title "Terminator 3: Rise of the Machines"],
        datom![208, :movie/year 2003],
        datom![208, :movie/director 127],
        datom![208, :movie/cast 101],
        datom![208, :movie/cast 128],
        datom![208, :movie/cast 129],
        datom![209, :movie/title "Rambo: First Blood Part II"],
        datom![209, :movie/year 1985],
        datom![209, :movie/director 130],
        datom![209, :movie/cast 105],
        datom![209, :movie/cast 106],
        datom![209, :movie/cast 131],
        datom![209, :movie/sequel 210],
        datom![210, :movie/title "Rambo III"],
        datom![210, :movie/year 1988],
        datom![210, :movie/director 132],
        datom![210, :movie/cast 105],
        datom![210, :movie/cast 106],
        datom![210, :movie/cast 133],
        datom![211, :movie/title "Predator 2"],
        datom![211, :movie/year 1990],
        datom![211, :movie/director 134],
        datom![211, :movie/cast 113],
        datom![211, :movie/cast 114],
        datom![211, :movie/cast 135],
        datom![212, :movie/title "Lethal Weapon 2"],
        datom![212, :movie/year 1989],
        datom![212, :movie/director 111],
        datom![212, :movie/cast 112],
        datom![212, :movie/cast 113],
        datom![212, :movie/cast 136],
        datom![212, :movie/sequel 213],
        datom![213, :movie/title "Lethal Weapon 3"],
        datom![213, :movie/year 1992],
        datom![213, :movie/director 111],
        datom![213, :movie/cast 112],
        datom![213, :movie/cast 113],
        datom![213, :movie/cast 136],
        datom![214, :movie/title "Alien"],
        datom![214, :movie/year 1979],
        datom![214, :movie/director 137],
        datom![214, :movie/cast 138],
        datom![214, :movie/cast 139],
        datom![214, :movie/cast 140],
        datom![214, :movie/sequel 215],
        datom![215, :movie/title "Aliens"],
        datom![215, :movie/year 1986],
        datom![215, :movie/director 100],
        datom![215, :movie/cast 139],
        datom![215, :movie/cast 141],
        datom![215, :movie/cast 103],
        datom![216, :movie/title "Mad Max"],
        datom![216, :movie/year 1979],
        datom![216, :movie/director 142],
        datom![216, :movie/cast 112],
        datom![216, :movie/cast 143],
        datom![216, :movie/cast 144],
        datom![216, :movie/sequel 217],
        datom![217, :movie/title "Mad Max 2"],
        datom![217, :movie/year 1981],
        datom![217, :movie/director 142],
        datom![217, :movie/cast 112],
        datom![217, :movie/cast 145],
        datom![217, :movie/cast 146],
        datom![217, :movie/sequel 218],
        datom![218, :movie/title "Mad Max Beyond Thunderdome"],
        datom![218, :movie/year 1985],
        datom![218, :movie/director "user"],
        datom![218, :movie/director 147],
        datom![218, :movie/cast 112],
        datom![218, :movie/cast 148],
        datom![219, :movie/title "Braveheart"],
        datom![219, :movie/year 1995],
        datom![219, :movie/director 112],
        datom![219, :movie/cast 112],
        datom![219, :movie/cast 149],
    ] };
}
