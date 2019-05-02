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
    borrow::Cow,
    cmp::Eq,
    error::Error,
    fmt::{self, Display, Formatter},
    str::{self, FromStr, Utf8Error},
};

use nom::{count, do_parse, map_res, named, peek, switch, tag, take, take_until_and_consume};

#[derive(Clone, Debug, PartialEq)]
pub enum RespData {
    SimpleString(Cow<'static, str>),
    Error(Cow<'static, str>),
    Integer(i64),
    BulkString(Cow<'static, str>),
    Nil,
    Array(Vec<RespData>),
}

impl Eq for RespData {}

pub struct SimpleStringRef<'a>(pub &'a str);

impl<'a> Display for SimpleStringRef<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "+{}\r\n", self.0)
    }
}

pub struct ErrorRef<'a>(pub &'a str);

impl<'a> Display for ErrorRef<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "-{}\r\n", self.0)
    }
}

pub struct BulkStringRef<'a>(pub &'a str);

impl<'a> Display for BulkStringRef<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        static PREFIXES: [&'static str; 33] = [
            "$0\r\n",
            "$1\r\n",
            "$2\r\n",
            "$3\r\n",
            "$4\r\n",
            "$5\r\n",
            "$6\r\n",
            "$7\r\n",
            "$8\r\n",
            "$9\r\n",
            "$10\r\n",
            "$11\r\n",
            "$12\r\n",
            "$13\r\n",
            "$14\r\n",
            "$15\r\n",
            "$16\r\n",
            "$17\r\n",
            "$18\r\n",
            "$19\r\n",
            "$20\r\n",
            "$21\r\n",
            "$22\r\n",
            "$23\r\n",
            "$24\r\n",
            "$25\r\n",
            "$26\r\n",
            "$27\r\n",
            "$28\r\n",
            "$29\r\n",
            "$30\r\n",
            "$31\r\n",
            "$32\r\n",
        ];

        if self.0.len() < PREFIXES.len() {
            write!(f, "{}{}\r\n", PREFIXES[self.0.len()], self.0)
        } else {
            write!(f, "${}\r\n{}\r\n", self.0.len(), self.0)
        }
    }
}

mod parse {
    use super::*;
    use nom::{
        alt, call, count, do_parse, map_res, named, switch, tag, take, take_until_and_consume,
    };

    named!(simple_string<&str, RespData>, do_parse!(
        data: take_until_and_consume!("\r\n") >>
        (RespData::SimpleString(Cow::Owned(data.into())))
    ));

    named!(error<&str, RespData>, do_parse!(
        data: take_until_and_consume!("\r\n") >>
        (RespData::Error(Cow::Owned(data.into())))
    ));

    named!(integer<&str, RespData>, do_parse!(
        value: map_res!(take_until_and_consume!("\r\n"), str::parse) >>
        (RespData::Integer(value))
    ));

    named!(bulk_string<&str, RespData>, do_parse!(
        len: map_res!(take_until_and_consume!("\r\n"), str::parse::<usize>) >>
        data: take!(len) >>
        tag!("\r\n") >>
        (RespData::BulkString(Cow::Owned(data.into())))
    ));

    named!(nil<&str, RespData>, do_parse!(
        tag!("-1\r\n") >>
        (RespData::Nil)
    ));

    named!(array<&str, RespData>, do_parse!(
        len: map_res!(take_until_and_consume!("\r\n"), str::parse::<usize>) >>
        results: count!(resp, len) >>
        (RespData::Array(results))
    ));

    named!(pub resp<&str, RespData>,
        switch!(take!(1),
            "+" => call!(simple_string) |
            "-" => call!(error) |
            ":" => call!(integer) |
            "$" => alt!(call!(nil) | call!(bulk_string)) |
            "*" => call!(array)
        )
    );
} // mod parse

fn split_trim(bytes: &[u8]) -> Result<Vec<String>, Utf8Error> {
    Ok(str::from_utf8(bytes)?
        .split_whitespace()
        .map(|s| s.trim())
        .map(String::from)
        .collect())
}

named!(pub parse_client_message<&[u8], Vec<String>>, switch!(peek!(take!(1)),
    b"*" => do_parse!(
        tag!("*") >>
        len: map_res!(
            map_res!(
                take_until_and_consume!("\r\n"),
                str::from_utf8
            ),
            str::parse::<usize>
        ) >>
        elems: count!(do_parse!(
            tag!("$") >>
            len: map_res!(
                map_res!(
                    take_until_and_consume!("\r\n"),
                    str::from_utf8
                ),
                str::parse::<usize>
            ) >>
            data: map_res!(take!(len), str::from_utf8) >>
            tag!("\r\n") >>
            (String::from(data))
        ), len) >>
        (elems)
    ) |
    _ => map_res!(
        take_until_and_consume!("\n"),
        split_trim
    )
));

impl FromStr for RespData {
    type Err = ParseRespError;

    fn from_str(s: &str) -> Result<RespData, ParseRespError> {
        match parse::resp(s) {
            Ok((rem, res)) => {
                if rem.is_empty() {
                    Ok(res)
                } else {
                    Err(ParseRespError::TrailingData)
                }
            }
            Err(e) => {
                if e.is_incomplete() {
                    Err(ParseRespError::Incomplete)
                } else {
                    Err(ParseRespError::Other)
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum ParseRespError {
    Incomplete,
    TrailingData,
    Other,
}

impl Error for ParseRespError {}

impl Display for ParseRespError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use ParseRespError::*;

        match self {
            Incomplete => write!(f, "incomplete parse"),
            TrailingData => write!(f, "trailing data"),
            Other => write!(f, "unknown"),
        }
    }
}

impl Display for RespData {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use RespData::*;

        match self {
            SimpleString(s) => SimpleStringRef(&s).fmt(f),
            Error(e) => ErrorRef(&e).fmt(f),
            Integer(i) => write!(f, ":{}\r\n", i),
            BulkString(s) => BulkStringRef(&s).fmt(f),
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
        fmt_eq(&SimpleString(Cow::Borrowed("OK")), "+OK\r\n");
    }

    #[test]
    fn fmt_error() {
        fmt_eq(&Error(Cow::Borrowed("Error message")), "-Error message\r\n");

        fmt_eq(
            &Error(Cow::Borrowed("ERR unknown command 'foobar'")),
            "-ERR unknown command 'foobar'\r\n",
        );

        fmt_eq(
            &Error(Cow::Borrowed(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
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
        fmt_eq(&BulkString(Cow::Borrowed("foobar")), "$6\r\nfoobar\r\n");

        fmt_eq(&BulkString(Cow::Borrowed("")), "$0\r\n\r\n");
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
                BulkString(Cow::Borrowed("foo")),
                BulkString(Cow::Borrowed("bar")),
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
                BulkString(Cow::Borrowed("foobar")),
            ]),
            "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n",
        );

        fmt_eq(
            &Array(vec![
                BulkString(Cow::Borrowed("foo")),
                Nil,
                BulkString(Cow::Borrowed("bar")),
            ]),
            "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n",
        );

        fmt_eq(
            &Array(vec![
                BulkString(Cow::Borrowed("LLEN")),
                BulkString(Cow::Borrowed("mylist")),
            ]),
            "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
        )
    }

    fn parse_eq(s: &str, expected: &RespData) {
        assert_eq!(&s.parse::<RespData>().unwrap(), expected);
    }

    #[test]
    fn parse_simple_string() {
        parse_eq("+OK\r\n", &SimpleString(Cow::Borrowed("OK")));
    }

    #[test]
    fn parse_error() {
        parse_eq("-Error message\r\n", &Error(Cow::Borrowed("Error message")));

        parse_eq(
            "-ERR unknown command 'foobar'\r\n",
            &Error(Cow::Borrowed("ERR unknown command 'foobar'")),
        );

        parse_eq(
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            &Error(Cow::Borrowed(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
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
        parse_eq("$6\r\nfoobar\r\n", &BulkString(Cow::Borrowed("foobar")));

        parse_eq("$0\r\n\r\n", &BulkString(Cow::Borrowed("")));
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
                BulkString(Cow::Borrowed("foo")),
                BulkString(Cow::Borrowed("bar")),
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
                BulkString(Cow::Borrowed("foobar")),
            ]),
        );

        parse_eq(
            "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n",
            &Array(vec![
                BulkString(Cow::Borrowed("foo")),
                Nil,
                BulkString(Cow::Borrowed("bar")),
            ]),
        );

        parse_eq(
            "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
            &Array(vec![
                BulkString(Cow::Borrowed("LLEN")),
                BulkString(Cow::Borrowed("mylist")),
            ]),
        )
    }

    #[test]
    fn parse_message() {
        let msg = b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
        let (rest, parsed) = parse_client_message(msg).unwrap();

        assert!(rest.is_empty());
        assert_eq!(parsed, vec![Cow::Borrowed("LLEN"), Cow::Borrowed("mylist")])
    }

    #[test]
    fn parse_inline() {
        let msg = b"LLEN mylist\r\n";
        let (rest, parsed) = parse_client_message(msg).unwrap();

        assert!(rest.is_empty());
        assert_eq!(parsed, vec![Cow::Borrowed("LLEN"), Cow::Borrowed("mylist")])
    }
}
