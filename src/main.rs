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

mod db;
mod hash_table;
mod resp;
mod sync;

use crate::{db::Database, resp::RespData};

use std::{env, net::{Ipv6Addr, SocketAddr, SocketAddrV6}, str};

use bytes::{BufMut, BytesMut};
use tokio::{
    codec::{Decoder, Encoder, Framed},
    io::{self, ErrorKind},
    net::tcp::TcpListener,
    prelude::{*, future::*},
};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

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
                    .map_err(|e| eprintln!("couldn't read response: {}", e))
                    .and_then(move |(cmd, args)| handle(&db, cmd, args))
                    .map_err(|_| io::Error::new(ErrorKind::Other, ""))
                    .forward(writer)
                    .map(|_| ())
                    .map_err(|e| eprintln!("couldn't write response: {}", e)),
            )
        });

    tokio::run(server);
}

fn handle(database: &Database, mut cmd: Vec<u8>, args: Vec<Vec<u8>>) -> Box<dyn Future<Item = RespData, Error = ()> + Send> {
    for ch in cmd.iter_mut() {
        *ch = (*ch as char).to_ascii_lowercase() as u8;
    }

    match cmd.as_slice() {
        b"decr" => Box::new(handle_decr(database, args)),
        b"decrby" => Box::new(handle_decrby(database, args)),
        b"get" => Box::new(handle_get(database, args)),
        b"getset" => Box::new(handle_getset(database, args)),
        b"incr" => Box::new(handle_incr(database, args)),
        b"incrby" => Box::new(handle_incrby(database, args)),
        b"set" => Box::new(handle_set(database, args)),
        b"ping" => Box::new(handle_ping(args)),
        _ => Box::new(future::ok::<RespData, ()>(RespData::Error("unrecognized command".into())))
    }
}

fn handle_decr(database: &Database, args: Vec<Vec<u8>>) -> impl Future<Item = RespData, Error = ()> {
    if args.len() != 1 {
        return Either::A(future::ok::<RespData, ()>(RespData::Error("too many/too few arguments for DECR".into())));
    }

    let mut iter = args.into_iter();
    let key = iter.next().unwrap();

    Either::B(database.decr(key))
}

fn handle_decrby(database: &Database, args: Vec<Vec<u8>>) -> impl Future<Item = RespData, Error = ()> {
    if args.len() != 2 {
        return Either::A(future::ok::<RespData, ()>(RespData::Error("too many/too few arguments for DECRBY".into())));
    }

    let mut iter = args.into_iter();
    let key = iter.next().unwrap();
    let decrement_str = iter.next().unwrap();
    let decrement = str::from_utf8(&decrement_str).unwrap().parse().unwrap();

    Either::B(database.decrby(key, decrement))
}

fn handle_get(database: &Database, args: Vec<Vec<u8>>) -> impl Future<Item = RespData, Error = ()> {
    if args.len() != 1 {
        return Either::A(future::ok::<RespData, ()>(RespData::Error("too many/too few arguments for GET".into())));
    }

    let mut iter = args.into_iter();
    let key = iter.next().unwrap();

    Either::B(database.get(key))
}

fn handle_getset(database: &Database, args: Vec<Vec<u8>>) -> impl Future<Item = RespData, Error = ()> {
    if args.len() != 2 {
        return Either::A(future::ok::<RespData, ()>(RespData::Error("too many/too few arguments for GETSET".into())));
    }

    let mut iter = args.into_iter();
    let key = iter.next().unwrap();
    let value = iter.next().unwrap();

    Either::B(database.getset(key, value))
}

fn handle_incr(database: &Database, args: Vec<Vec<u8>>) -> impl Future<Item = RespData, Error = ()> {
    if args.len() != 1 {
        return Either::A(future::ok::<RespData, ()>(RespData::Error("too many/too few arguments for INCR".into())));
    }

    let mut iter = args.into_iter();
    let key = iter.next().unwrap();

    Either::B(database.incr(key))
}

fn handle_incrby(database: &Database, args: Vec<Vec<u8>>) -> impl Future<Item = RespData, Error = ()> {
    if args.len() != 2 {
        return Either::A(future::ok::<RespData, ()>(RespData::Error("too many/too few arguments for INCRBY".into())));
    }

    let mut iter = args.into_iter();
    let key = iter.next().unwrap();
    let decrement_str = iter.next().unwrap();
    let decrement = str::from_utf8(&decrement_str).unwrap().parse().unwrap();

    Either::B(database.decrby(key, decrement))
}

fn handle_set(database: &Database, args: Vec<Vec<u8>>) -> impl Future<Item = RespData, Error = ()> {
    if args.len() != 2 {
        return Either::A(future::ok::<RespData, ()>(RespData::Error("too many/too few arguments for SET".into())));
    }

    let mut iter = args.into_iter();
    let key = iter.next().unwrap();
    let value = iter.next().unwrap();

    Either::B(database.set(key, value))
}

fn handle_ping(args: Vec<Vec<u8>>) -> impl Future<Item = RespData, Error = ()> {
    if args.len() > 1 {
        return future::ok::<RespData, ()>(RespData::Error("too many/too few arguments for PING".into()));
    }

    let mut iter = args.into_iter();

    if let Some(msg) = iter.next() {
        future::ok::<RespData, ()>(RespData::BulkString(msg))
    } else {
        future::ok::<RespData, ()>(RespData::SimpleString("PONG".into()))
    }
}

struct RespCodec {
    start_idx: usize,
}

impl RespCodec {
    fn new() -> RespCodec {
        RespCodec { start_idx: 0 }
    }
}

impl Encoder for RespCodec {
    type Item = RespData;
    type Error = io::Error;

    fn encode(&mut self, data: RespData, dest: &mut BytesMut) -> Result<(), Self::Error> {
        let to_write = data.serialize()?;
        dest.reserve(to_write.len());
        dest.put_slice(&to_write);

        Ok(())
    }
}

impl Decoder for RespCodec {
    type Item = (Vec<u8>, Vec<Vec<u8>>);
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(_) = src[self.start_idx..].iter().position(|b| *b == b'\n') {
            match resp::parse_client_message(src.as_ref()) {
                Ok((rest, msg)) => {
                    let mut iter = msg.into_iter();
                    let command = (*iter.next().unwrap()).into();

                    let owned = iter
                        .map(|word| (*word).into())
                        .collect();

                    let to_trim = src.len() - rest.len();
                    src.advance(to_trim);
                    self.start_idx = 0;

                    Ok(Some((command, owned)))
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
