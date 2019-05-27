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
    cmp::Eq,
    error::Error,
    fmt::{self, Display, Formatter},
    str::{self, FromStr},
    io::{self, Write},
};

use nom::{count, do_parse, map, map_res, named, peek, switch, tag, take, take_until_and_consume};

#[derive(Clone, Debug, PartialEq)]
pub enum RespData {
    SimpleString(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    BulkString(Vec<u8>),
    Nil,
    Array(Vec<RespData>),
}

impl RespData {
    pub fn ok() -> RespData {
        RespData::SimpleString("OK".into())
    }

    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(self.serialized_len());
        self.write_to(&mut buffer)?;

        Ok(buffer)
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            RespData::SimpleString(s) => {
                writer.write_all(b"+")?;
                writer.write_all(s)?;
                writer.write_all(b"\r\n")
            },
            RespData::Error(e) => {
                writer.write_all(b"-")?;
                writer.write_all(e)?;
                writer.write_all(b"\r\n")
            },
            RespData::Integer(i) => {
                writer.write_all(b":")?;
                write!(writer, "{}", i)?;
                writer.write_all(b"\r\n")
            },
            RespData::BulkString(s) => {
                write!(writer, "${}\r\n", s.len())?;
                writer.write_all(s)?;
                writer.write_all(b"\r\n")
            },
            RespData::Nil => {
                writer.write_all(b"$-1\r\n")
            }
            RespData::Array(a) => {
                write!(writer, "*{}\r\n", a.len())?;

                for elem in a.iter() {
                    elem.write_to(writer)?;
                }

                Ok(())
            }
        }
    }

    fn serialized_len(&self) -> usize {
        match self {
            RespData::SimpleString(s) | RespData::Error(s) => s.len() + 3,
            RespData::Integer(i) => {
                let num_ser_bits = serialized_len(i.abs() as usize);

                num_ser_bits + 3 + if *i < 0 { 1 } else { 0 }
            }
            RespData::BulkString(s) =>
                s.len() + serialized_len(s.len()) + 5,
            RespData::Nil => 5,
            RespData::Array(a) =>
                a.iter()
                    .map(RespData::serialized_len)
                    .fold(3 + serialized_len(a.len()), |x, y| x + y)
        }
    }
}

fn serialized_len(num: usize) -> usize {
    if num == 0 {
        return 1;
    }

    ((num + 1) as f64).log10().ceil() as usize
}

impl Eq for RespData {}

mod parse {
    use super::*;
    use nom::{
        alt, call, count, do_parse, map_res, named, switch, tag, take, take_until_and_consume,
    };

    named!(simple_string<RespData>, do_parse!(
        data: take_until_and_consume!("\r\n") >>
        (RespData::SimpleString(data.into()))
    ));

    named!(error<RespData>, do_parse!(
        data: take_until_and_consume!("\r\n") >>
        (RespData::Error(data.into()))
    ));

    named!(integer<RespData>, do_parse!(
        value: map_res!(map_res!(take_until_and_consume!("\r\n"), str::from_utf8), str::parse::<i64>) >>
        (RespData::Integer(value))
    ));

    named!(bulk_string<RespData>, do_parse!(
        len: map_res!(map_res!(take_until_and_consume!("\r\n"), str::from_utf8), str::parse::<usize>) >>
        data: take!(len) >>
        tag!("\r\n") >>
        (RespData::BulkString(data.into()))
    ));

    named!(nil<RespData>, do_parse!(
        tag!("-1\r\n") >>
        (RespData::Nil)
    ));

    named!(array<RespData>, do_parse!(
        len: map_res!(map_res!(take_until_and_consume!("\r\n"), str::from_utf8), str::parse::<usize>) >>
        results: count!(resp, len) >>
        (RespData::Array(results))
    ));

    named!(pub resp<RespData>,
        switch!(take!(1),
            b"+" => call!(simple_string) |
            b"-" => call!(error) |
            b":" => call!(integer) |
            b"$" => alt!(call!(nil) | call!(bulk_string)) |
            b"*" => call!(array)
        )
    );
} // mod parse

fn split_trim(mut bytes: &[u8]) -> Vec<&[u8]> {
    if let Some(idx) = bytes.iter().cloned().position(is_not_whitespace) {
        bytes = &bytes[idx..];
    } else {
        return Vec::new();
    }

    let mut vec = Vec::new();

    while !bytes.is_empty() {
        let first_whitespace = bytes
            .iter()
            .cloned()
            .position(is_whitespace)
            .unwrap_or(bytes.len());

        vec.push(&bytes[..first_whitespace]);
        bytes = &bytes[first_whitespace..];

        if let Some(i) = bytes
            .iter()
            .cloned()
            .position(is_not_whitespace)
        {
            bytes = &bytes[i..];
        } else {
            bytes = &[];
        }
    }

    vec
}

fn is_not_whitespace(byte: u8) -> bool {
    !is_whitespace(byte)
}

fn is_whitespace(byte: u8) -> bool {
    // space, horizontal tab, line feed, form feed, or carriage return
    match byte {
        b' ' | b'\t' | 10 | 12 | b'\r' => true,
        _ => false,
    }
}

named!(pub parse_client_message<Vec<&[u8]>>, switch!(peek!(take!(1)),
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
            data: take!(len) >>
            tag!("\r\n") >>
            (data)
        ), len) >>
        (elems)
    ) |
    _ => map!(
        take_until_and_consume!("\n"),
        split_trim
    )
));

impl FromStr for RespData {
    type Err = ParseRespError;

    fn from_str(s: &str) -> Result<RespData, ParseRespError> {
        match parse::resp(s.as_bytes()) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use RespData::*;

    fn fmt_eq(resp: &RespData, expected: &str) {
        let serialized = resp.serialize().unwrap();

        assert_eq!(serialized.len(), resp.serialized_len());
        assert_eq!(serialized, expected.as_bytes());
    }

    #[test]
    fn fmt_simple_string() {
        fmt_eq(&SimpleString("OK".into()), "+OK\r\n");
    }

    #[test]
    fn fmt_error() {
        fmt_eq(&Error("Error message".into()), "-Error message\r\n");

        fmt_eq(
            &Error("ERR unknown command 'foobar'".into()),
            "-ERR unknown command 'foobar'\r\n",
        );

        fmt_eq(
            &Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
        );
    }

    #[test]
    fn iserlen() {
        for i in 0..10 {
            println!("i = {}", i);
            assert_eq!(serialized_len(i), 1);
        }

        for i in 10..100 {
            println!("i = {}", i);
            assert_eq!(serialized_len(i), 2);
        }

        for i in 100..1000 {
            println!("i = {}", i);
            assert_eq!(serialized_len(i), 3);
        }
    }

    #[test]
    fn fmt_integer() {
        fmt_eq(&Integer(0), ":0\r\n");

        fmt_eq(&Integer(1000), ":1000\r\n");

        fmt_eq(&Integer(48293), ":48293\r\n");
    }

    #[test]
    fn fmt_bulk_string() {
        fmt_eq(&BulkString("foobar".into()), "$6\r\nfoobar\r\n");

        fmt_eq(&BulkString("".into()), "$0\r\n\r\n");
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
                BulkString("foo".into()),
                BulkString("bar".into()),
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
                BulkString("foobar".into()),
            ]),
            "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n",
        );

        fmt_eq(
            &Array(vec![
                BulkString("foo".into()),
                Nil,
                BulkString("bar".into()),
            ]),
            "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n",
        );

        fmt_eq(
            &Array(vec![
                BulkString("LLEN".into()),
                BulkString("mylist".into()),
            ]),
            "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
        )
    }

    fn parse_eq(s: &str, expected: &RespData) {
        assert_eq!(&s.parse::<RespData>().unwrap(), expected);
    }

    #[test]
    fn parse_simple_string() {
        parse_eq("+OK\r\n", &SimpleString("OK".into()));
    }

    #[test]
    fn parse_error() {
        parse_eq("-Error message\r\n", &Error("Error message".into()));

        parse_eq(
            "-ERR unknown command 'foobar'\r\n",
            &Error("ERR unknown command 'foobar'".into()),
        );

        parse_eq(
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            &Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
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
        parse_eq("$6\r\nfoobar\r\n", &BulkString("foobar".into()));

        parse_eq("$0\r\n\r\n", &BulkString("".into()));
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
                BulkString("foo".into()),
                BulkString("bar".into()),
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
                BulkString("foobar".into()),
            ]),
        );

        parse_eq(
            "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n",
            &Array(vec![
                BulkString("foo".into()),
                Nil,
                BulkString("bar".into()),
            ]),
        );

        parse_eq(
            "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
            &Array(vec![
                BulkString("LLEN".into()),
                BulkString("mylist".into()),
            ]),
        )
    }

    #[test]
    fn parse_message() {
        let msg = b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
        let (rest, parsed) = parse_client_message(msg).unwrap();

        assert!(rest.is_empty());
        assert_eq!(parsed, vec!["LLEN".as_bytes(), "mylist".as_bytes()])
    }

    #[test]
    fn parse_inline() {
        let msg = b"LLEN mylist\r\n";
        let (rest, parsed) = parse_client_message(msg).unwrap();

        assert!(rest.is_empty());
        assert_eq!(parsed, vec!["LLEN".as_bytes(), "mylist".as_bytes()])
    }

    #[test]
    fn split_trim_cases() {
        assert_eq!(
            split_trim("hello world!".as_bytes()),
            vec!["hello".as_bytes(), "world!".as_bytes()]
        );

        assert_eq!(
            split_trim("   hello   world!   ".as_bytes()),
            vec!["hello".as_bytes(), "world!".as_bytes()]
        );

        assert_eq!(
            split_trim("   hello   world!".as_bytes()),
            vec!["hello".as_bytes(), "world!".as_bytes()]
        );

        assert_eq!(
            split_trim("hello   world!   ".as_bytes()),
            vec!["hello".as_bytes(), "world!".as_bytes()]
        );

        assert_eq!(
            split_trim("   hello world!   ".as_bytes()),
            vec!["hello".as_bytes(), "world!".as_bytes()]
        );

        assert_eq!(
            split_trim("   hello world!".as_bytes()),
            vec!["hello".as_bytes(), "world!".as_bytes()]
        );

        assert_eq!(
            split_trim("hello   world!".as_bytes()),
            vec!["hello".as_bytes(), "world!".as_bytes()]
        );

        assert_eq!(
            split_trim("hello world!   ".as_bytes()),
            vec!["hello".as_bytes(), "world!".as_bytes()]
        );
    }
}
