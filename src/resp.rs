// MIT License
//
// Copyright (c) 2019 Gregory Meyer
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation files
// (the "Software"), to deal in the Software without restriction,
// including without limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of the Software,
// and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
// BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
// ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::{
    cmp::{Eq, PartialEq},
    error::Error,
    fmt::{self, Display, Formatter},
    str::{self, FromStr},
};

use nom::{alt, call, count, do_parse, map_res, named, switch, tag, take, take_until_and_consume};

#[derive(Clone, Debug)]
pub enum RespData {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Nil,
    Array(Vec<RespData>),
}

impl PartialEq for RespData {
    fn eq(&self, other: &RespData) -> bool {
        use RespData::*;

        match self {
            SimpleString(lhs) => {
                if let SimpleString(rhs) = other {
                    lhs == rhs
                } else {
                    false
                }
            }
            Error(lhs) => {
                if let Error(rhs) = other {
                    lhs == rhs
                } else {
                    false
                }
            }
            Integer(lhs) => {
                if let Integer(rhs) = other {
                    lhs == rhs
                } else {
                    false
                }
            }
            BulkString(lhs) => {
                if let BulkString(rhs) = other {
                    lhs == rhs
                } else {
                    false
                }
            }
            Nil => {
                if let Nil = other {
                    true
                } else {
                    false
                }
            }
            Array(lhs) => {
                if let Array(rhs) = other {
                    lhs == rhs
                } else {
                    false
                }
            }
        }
    }
}

impl Eq for RespData {}

named!(parse_simple_string<&str, RespData>, do_parse!(
    data: take_until_and_consume!("\r\n") >>
    (RespData::SimpleString(data.into()))
));

named!(parse_error<&str, RespData>, do_parse!(
    data: take_until_and_consume!("\r\n") >>
    (RespData::Error(data.into()))
));

named!(parse_integer<&str, RespData>, do_parse!(
    value: map_res!(take_until_and_consume!("\r\n"), str::parse) >>
    (RespData::Integer(value))
));

named!(parse_bulk_string<&str, RespData>, do_parse!(
    len: map_res!(take_until_and_consume!("\r\n"), str::parse::<usize>) >>
    data: take!(len) >>
    tag!("\r\n") >>
    (RespData::BulkString(data.into()))
));

named!(parse_nil<&str, RespData>, do_parse!(
    tag!("-1\r\n") >>
    (RespData::Nil)
));

named!(parse_array<&str, RespData>, do_parse!(
    len: map_res!(take_until_and_consume!("\r\n"), str::parse::<usize>) >>
    results: count!(parse_resp, len) >>
    (RespData::Array(results))
));

named!(parse_resp<&str, RespData>,
    switch!(take!(1),
        "+" => call!(parse_simple_string) |
        "-" => call!(parse_error) |
        ":" => call!(parse_integer) |
        "$" => alt!(call!(parse_nil) | call!(parse_bulk_string)) |
        "*" => call!(parse_array)
    )
);

impl FromStr for RespData {
    type Err = ParseRespError;

    fn from_str(s: &str) -> Result<RespData, ParseRespError> {
        match parse_resp(s) {
            Ok((rem, res)) => {
                if rem.is_empty() {
                    Ok(res)
                } else {
                    Err(ParseRespError())
                }
            }
            Err(_) => Err(ParseRespError()),
        }
    }
}

#[derive(Debug)]
pub struct ParseRespError();

impl Error for ParseRespError {}

impl Display for ParseRespError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "unexpected character in string")
    }
}

impl Display for RespData {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use RespData::*;

        match self {
            SimpleString(s) => write!(f, "+{}\r\n", s),
            Error(e) => write!(f, "-{}\r\n", e),
            Integer(i) => write!(f, ":{}\r\n", i),
            BulkString(i) => write!(f, "${}\r\n{}\r\n", i.len(), i),
            Nil => write!(f, "$-1\r\n"),
            Array(d) => {
                write!(f, "*{}\r\n", d.len())?;

                for elem in d.iter() {
                    elem.fmt(f)?;
                }

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use RespData::*;

    fn fmt_eq(resp: &RespData, expected: &str) {
        let actual = format!("{}", resp);
        assert_eq!(actual, expected);
    }

    #[test]
    fn fmt_simple_string() {
        fmt_eq(&SimpleString("OK".to_string()), "+OK\r\n");
    }

    #[test]
    fn fmt_error() {
        fmt_eq(&Error("Error message".to_string()), "-Error message\r\n");

        fmt_eq(
            &Error("ERR unknown command 'foobar'".to_string()),
            "-ERR unknown command 'foobar'\r\n",
        );

        fmt_eq(
            &Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
        );
    }

    #[test]
    fn fmt_integer() {
        fmt_eq(&Integer(0), ":0\r\n");

        fmt_eq(&Integer(1000), ":1000\r\n");

        fmt_eq(&Integer(48293), ":48293\r\n");
    }

    #[test]
    fn fmt_bulk_string() {
        fmt_eq(&BulkString("foobar".to_string()), "$6\r\nfoobar\r\n");

        fmt_eq(&BulkString("".to_string()), "$0\r\n\r\n");
    }

    #[test]
    fn fmt_null() {
        fmt_eq(&Nil, "$-1\r\n");
    }

    #[test]
    fn fmt_array() {
        fmt_eq(&Array(Vec::new()), "*0\r\n");

        fmt_eq(
            &Array(vec![
                BulkString("foo".to_string()),
                BulkString("bar".to_string()),
            ]),
            "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        );

        fmt_eq(
            &Array(vec![Integer(1), Integer(2), Integer(3)]),
            "*3\r\n:1\r\n:2\r\n:3\r\n",
        );

        fmt_eq(
            &Array(vec![
                Integer(1),
                Integer(2),
                Integer(3),
                Integer(4),
                BulkString("foobar".to_string()),
            ]),
            "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n",
        );

        fmt_eq(
            &Array(vec![
                BulkString("foo".to_string()),
                Nil,
                BulkString("bar".to_string()),
            ]),
            "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n",
        );

        fmt_eq(
            &Array(vec![
                BulkString("LLEN".to_string()),
                BulkString("mylist".to_string()),
            ]),
            "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
        )
    }

    fn parse_eq(s: &str, expected: &RespData) {
        assert_eq!(&s.parse::<RespData>().unwrap(), expected);
    }

    #[test]
    fn parse_simple_string() {
        parse_eq("+OK\r\n", &SimpleString("OK".to_string()));
    }

    #[test]
    fn parse_error() {
        parse_eq("-Error message\r\n", &Error("Error message".to_string()));

        parse_eq(
            "-ERR unknown command 'foobar'\r\n",
            &Error("ERR unknown command 'foobar'".to_string()),
        );

        parse_eq(
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            &Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        );
    }

    #[test]
    fn parse_integer() {
        parse_eq(":0\r\n", &Integer(0));

        parse_eq(":1000\r\n", &Integer(1000));

        parse_eq(":48293\r\n", &Integer(48293));
    }

    #[test]
    fn parse_bulk_string() {
        parse_eq("$6\r\nfoobar\r\n", &BulkString("foobar".to_string()));

        parse_eq("$0\r\n\r\n", &BulkString("".to_string()));
    }

    #[test]
    fn parse_null() {
        parse_eq("$-1\r\n", &Nil);
    }

    #[test]
    fn parse_array() {
        parse_eq("*0\r\n", &Array(Vec::new()));

        parse_eq(
            "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            &Array(vec![
                BulkString("foo".to_string()),
                BulkString("bar".to_string()),
            ]),
        );

        parse_eq(
            "*3\r\n:1\r\n:2\r\n:3\r\n",
            &Array(vec![Integer(1), Integer(2), Integer(3)]),
        );

        parse_eq(
            "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n",
            &Array(vec![
                Integer(1),
                Integer(2),
                Integer(3),
                Integer(4),
                BulkString("foobar".to_string()),
            ]),
        );

        parse_eq(
            "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n",
            &Array(vec![
                BulkString("foo".to_string()),
                Nil,
                BulkString("bar".to_string()),
            ]),
        );

        parse_eq(
            "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
            &Array(vec![
                BulkString("LLEN".to_string()),
                BulkString("mylist".to_string()),
            ]),
        )
    }
}
