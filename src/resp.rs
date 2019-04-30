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
    fmt::{self, Display, Formatter},
    str::FromStr,
};

#[derive(Clone, Debug)]
pub enum RespData {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Null,
    Array(Vec<RespData>),
}

impl Display for RespData {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use RespData::*;

        match self {
            SimpleString(s) => write!(f, "+{}\r\n", s),
            Error(e) => write!(f, "-{}\r\n", e),
            Integer(i) => write!(f, ":{}\r\n", i),
            BulkString(i) => write!(f, "${}\r\n{}\r\n", i.len(), i),
            Null => write!(f, "$-1\r\n"),
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
        fmt_eq(&Null, "$-1\r\n");
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
                Null,
                BulkString("bar".to_string()),
            ]),
            "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n",
        );

        fmt_eq(
            &Array(vec![
                BulkString("LLEN".to_string()),
                BulkString("mylist".to_string()),
            ]),
            "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"
        )
    }
}
