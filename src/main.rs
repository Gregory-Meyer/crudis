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

mod database;
mod resp;

use database::Database;
use resp::RespData;

use std::{
    env,
    fmt::{self, Write as FmtWrite, Formatter},
    fmt::Display,
    io::Write,
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
};

use bytes::BytesMut;
use hashbrown::HashMap;
use tokio::{
    codec::{Decoder, Encoder, Framed},
    io::{self, ErrorKind},
    net::tcp::TcpListener,
    prelude::*,
};

use lazy_static::lazy_static;

struct RespCodec {
    start_idx: usize,
}

impl RespCodec {
    fn new() -> RespCodec {
        RespCodec { start_idx: 0 }
    }
}

impl Decoder for RespCodec {
    type Item = Vec<String>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(_) = src[self.start_idx..].iter().position(|b| *b == b'\n') {
            match resp::parse_client_message(src.as_ref()) {
                Ok((rest, msg)) => {
                    let to_trim = src.len() - rest.len();
                    src.advance(to_trim);
                    self.start_idx = 0;

                    Ok(Some(msg))
                }
                Err(e) => {
                    if e.is_incomplete() {
                        self.start_idx = src.len();

                        Ok(None)
                    } else {
                        Err(io::Error::new(
                            ErrorKind::InvalidData,
                            "invalid data in stream",
                        ))
                    }
                }
            }
        } else {
            Ok(None)
        }
    }
}

struct LengthFinder(usize);

impl Write for LengthFinder {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len();

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Encoder for RespCodec {
    type Item = RespData;
    type Error = io::Error;

    fn encode(&mut self, data: RespData, dest: &mut BytesMut) -> Result<(), Self::Error> {
        let mut length_finder = LengthFinder(0);
        write!(&mut length_finder, "{}", data).unwrap();
        dest.reserve(length_finder.0);

        write!(dest, "{}", data).unwrap();

        Ok(())
    }
}

type Handler = fn(&Database, &[String]) -> RespData;

lazy_static! {
    static ref COMMANDS: HashMap<&'static str, (usize, Handler)> = {
        let mut commands = HashMap::new();
        commands.insert("get", (1, handle_get as Handler));
        commands.insert("set", (2, handle_set as Handler));
        commands.insert("ping", (0, handle_ping as Handler));

        commands
    };
}

struct Command<'a>(&'a [String]);

impl<'a> Display for Command<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "`{}`, with args beginning with: ", self.0[0])?;

        for arg in self.0[1..].iter() {
            write!(f, "`{}`, ", arg)?;
        }

        Ok(())
    }
}

fn make_response(db: &Database, msg: &[String]) -> RespData {
    assert!(!msg.is_empty());

    let command = msg[0].to_lowercase();

    if let Some((arity, f)) = COMMANDS.get(command.as_str()) {
        if msg.len() != arity + 1 {
            let msg = format!("ERR wrong number of arguments for '{}' command", command);

            RespData::Error(msg)
        } else {
            f(db, &msg[1..])
        }
    } else {
        let msg = format!("ERR unknown command {}", Command(msg));

        RespData::Error(msg)
    }
}

fn handle_get(db: &Database, args: &[String]) -> RespData {
    db.get(args[0].as_str())
}

fn handle_set(db: &Database, args: &[String]) -> RespData {
    db.set(args[0].clone(), args[1].clone())
}

fn handle_ping(_: &Database, _: &[String]) -> RespData {
    RespData::SimpleString("PONG".to_string())
}

fn main() {
    let addr = env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or_else(|| {
            SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1),
                6379,
                0,
                0,
            ))
        });

    let listener = TcpListener::bind(&addr).expect("couldn't bind TCP listener");
    let db = Database::new();

    let server = listener
        .incoming()
        .map_err(|e| eprintln!("couldn't accept a TCP connection: {}", e))
        .for_each(move |sock| {
            let (writer, reader) = Framed::new(sock, RespCodec::new()).split();

            let db = db.clone();
            tokio::spawn(
                reader
                    .map(move |msg| make_response(&db, &msg))
                    .forward(writer)
                    .map(|_| ())
                    .map_err(|e| eprintln!("couldn't write response: {}", e)),
            )
        });

    tokio::run(server);
}
