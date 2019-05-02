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

use std::{
    env,
    fmt::Display,
    fmt::{self, Formatter},
    io::{ErrorKind, Read, Write},
    net::{Ipv6Addr, SocketAddr, SocketAddrV6, TcpListener, TcpStream},
    process, thread,
};

use nom::Context;

fn start() -> i32 {
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

    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("crudis: couldn't bind to {}: {}", addr, e);

            return 1;
        }
    };

    let db = Database::new();

    loop {
        let (socket, client_addr) = match listener.accept() {
            Ok((s, a)) => (s, a),
            Err(e) => {
                eprintln!("crudis: couldn't accept a connection: {}", e);

                continue;
            }
        };

        println!("crudis: accepted a connection from {}", client_addr);

        if let Err(e) = socket.set_nodelay(true) {
            eprintln!("crudis: couldn't disable Nagle's algorithm: {}", e);
        }

        if let Err(e) = socket.set_write_timeout(None) {
            eprintln!("crudis: couldn't disable socket write timeout: {}", e);
        }

        if let Err(e) = socket.set_read_timeout(None) {
            eprintln!("crudis: couldn't disable socket read timeout: {}", e);
        }

        let db = db.clone();
        thread::spawn(move || {
            client_loop(db, socket);

            println!("crudis: closed connection with {}", client_addr);
        });
    }

    0
}

fn client_loop(db: Database, mut socket: TcpStream) {
    let mut start_idx = 0;
    let mut buf = Vec::with_capacity(4096);

    loop {
        if let Some(_) = buf[start_idx..].iter().position(|b| *b == b'\n') {
            let (maybe_msg, new_start_idx) = parse_buffer(&mut buf);
            start_idx = new_start_idx;

            if let Some(msg) = maybe_msg {
                let response = make_response(&db, msg);

                if let Err(e) = socket.write_all(&response) {
                    if e.kind() == ErrorKind::ConnectionReset {
                        return;
                    }

                    eprintln!("crudis: couldn't write to socket: {:?}", e);
                }
            }
        } else {
            start_idx = buf.len();
            let new_data_start = buf.len();
            buf.extend_from_slice(&[0; 4096]);

            match socket.read(&mut buf[start_idx..]) {
                Ok(i) => {
                    if i == 0 {
                        return;
                    } else {
                        buf.truncate(new_data_start + i)
                    }
                }
                Err(e) => match e.kind() {
                    ErrorKind::ConnectionReset => return,
                    _ => eprintln!("crudis: failed to read from socket: {:?}", e),
                },
            }
        }
    }
}

fn parse_buffer(buf: &mut Vec<u8>) -> (Option<Vec<String>>, usize) {
    match resp::parse_client_message(buf.as_ref()) {
        Ok((rest, msg)) => {
            let to_trim = buf.len() - rest.len();
            buf.drain(..to_trim);

            (Some(msg), 0)
        }
        Err(e) => match e {
            nom::Err::Incomplete(_) => (None, buf.len()),
            nom::Err::Error(c) | nom::Err::Failure(c) => match c {
                Context::Code(i, _) => {
                    buf.drain(..buf.len() - i.len());

                    (None, 0)
                }
            },
        },
    }
}

fn main() {
    let retc = start();

    if retc != 0 {
        process::exit(retc);
    }
}

fn make_response(db: &Database, msg: Vec<String>) -> Vec<u8> {
    assert!(!msg.is_empty());

    let command = msg[0].to_lowercase();

    if let Some((arity, f)) = get_command(command.as_str()) {
        if (arity != -1) && (msg.len() != (arity as usize) + 1) {
            format!(
                "-ERR wrong number of arguments for '{}' command\r\n",
                command
            )
            .into_bytes()
        } else {
            let mut response = Vec::new();
            f(db, msg, &mut response);

            response
        }
    } else {
        format!("-ERR unknown command {}\r\n", Command(&msg)).into_bytes()
    }
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

type Handler = fn(&Database, Vec<String>, &mut Vec<u8>);

fn get_command(command: &str) -> Option<(isize, Handler)> {
    match command {
        "decr" => Some((1, handle_decr)),
        "decrby" => Some((2, handle_decrby)),
        "get" => Some((1, handle_get)),
        "getset" => Some((2, handle_getset)),
        "incr" => Some((1, handle_incr)),
        "incrby" => Some((2, handle_incrby)),
        "mget" => Some((-1, handle_mget)),
        "set" => Some((2, handle_set)),
        "setnx" => Some((2, handle_setnx)),
        "lindex" => Some((2, handle_lindex)),
        "llen" => Some((1, handle_llen)),
        "lpop" => Some((1, handle_lpop)),
        "lpush" => Some((2, handle_lpush)),
        "lrange" => Some((3, handle_lrange)),
        "lrem" => Some((3, handle_lrem)),
        "lset" => Some((3, handle_lset)),
        "ltrim" => Some((3, handle_ltrim)),
        "rpop" => Some((1, handle_rpop)),
        "rpush" => Some((2, handle_rpush)),
        "del" => Some((-1, handle_del)),
        "exists" => Some((1, handle_exists)),
        "ping" => Some((0, handle_ping)),
        _ => None,
    }
}

fn handle_decr(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let key = args.into_iter().nth(1).unwrap();

    db.decr(key, buf).unwrap();
}

fn handle_decrby(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let mut iter = args.into_iter().skip(1);
    let key = iter.next().unwrap();

    let decrement = match iter.next().unwrap().parse() {
        Ok(i) => i,
        Err(_) => {
            buf.write_all(b"-ERR value is not an integer or out of range\r\n")
                .unwrap();

            return;
        }
    };

    db.decrby(key, decrement, buf).unwrap();
}

fn handle_get(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    db.get(&args.into_iter().nth(1).unwrap(), buf).unwrap();
}

fn handle_getset(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    db.getset(args[0].clone(), args[1].clone(), buf).unwrap();
}

fn handle_incr(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    db.incr(args[0].clone(), buf).unwrap();
}

fn handle_incrby(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let increment = match args[1].parse() {
        Ok(i) => i,
        Err(_) => {
            buf.write_all(b"-ERR value is not an integer or out of range\r\n")
                .unwrap();

            return;
        }
    };

    db.incrby(args[0].clone(), increment, buf).unwrap();
}

fn handle_mget(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    db.mget(&args[1..], buf).unwrap();
}

fn handle_set(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let mut iter = args.into_iter().skip(1);
    let key = iter.next().unwrap();
    let value = iter.next().unwrap();

    db.set(key, value, buf).unwrap();
}

fn handle_setnx(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let mut iter = args.into_iter().skip(1);
    let key = iter.next().unwrap();
    let value = iter.next().unwrap();

    db.setnx(key, value, buf).unwrap();
}

fn handle_lindex(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let index = match args[2].parse() {
        Ok(i) => i,
        Err(_) => {
            buf.write_all(b"-ERR value is not an integer or out of range\r\n")
                .unwrap();

            return;
        }
    };

    db.lindex(args[1].as_str(), index, buf).unwrap();
}

fn handle_llen(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    db.llen(args[1].as_str(), buf).unwrap();
}

fn handle_lpop(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    db.lpop(args[1].as_str(), buf).unwrap();
}

fn handle_lpush(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let mut iter = args.into_iter().skip(1);
    let key = iter.next().unwrap();
    let value = iter.next().unwrap();

    db.lpush(key, value, buf).unwrap();
}

fn handle_lrange(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let start = match args[1].parse() {
        Ok(i) => i,
        Err(_) => {
            buf.write_all(b"-ERR value is not an integer or out of range\r\n")
                .unwrap();

            return;
        }
    };

    let stop = match args[2].parse() {
        Ok(i) => i,
        Err(_) => {
            buf.write_all(b"-ERR value is not an integer or out of range\r\n")
                .unwrap();

            return;
        }
    };

    db.lrange(args[1].as_str(), start, stop, buf).unwrap();
}

fn handle_lrem(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let count = match args[2].parse() {
        Ok(i) => i,
        Err(_) => {
            buf.write_all(b"-ERR value is not an integer or out of range\r\n")
                .unwrap();

            return;
        }
    };

    db.lrem(args[1].as_str(), count, args[3].as_str(), buf)
        .unwrap();
}

fn handle_lset(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let mut iter = args.into_iter().skip(1);
    let key = iter.next().unwrap();

    let index = match iter.next().unwrap().parse() {
        Ok(i) => i,
        Err(_) => {
            buf.write_all(b"-ERR value is not an integer or out of range\r\n")
                .unwrap();

            return;
        }
    };

    let value = iter.next().unwrap();

    db.lset(&key, index, value, buf).unwrap();
}

fn handle_ltrim(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let mut iter = args.into_iter().skip(1);
    let key = iter.next().unwrap();

    let start = match iter.next().unwrap().parse() {
        Ok(i) => i,
        Err(_) => {
            buf.write_all(b"-ERR value is not an integer or out of range\r\n")
                .unwrap();

            return;
        }
    };

    let stop = match iter.next().unwrap().parse() {
        Ok(i) => i,
        Err(_) => {
            buf.write_all(b"-ERR value is not an integer or out of range\r\n")
                .unwrap();

            return;
        }
    };

    db.ltrim(&key, start, stop, buf).unwrap();
}

fn handle_rpop(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    db.rpop(args[1].as_str(), buf).unwrap();
}

fn handle_rpush(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    let mut iter = args.into_iter().skip(1);
    let key = iter.next().unwrap();
    let value = iter.next().unwrap();

    db.rpush(key, value, buf).unwrap();
}

fn handle_del(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    db.del(&args, buf).unwrap();
}

fn handle_exists(db: &Database, args: Vec<String>, buf: &mut Vec<u8>) {
    db.exists(args[0].as_str(), buf).unwrap();
}

fn handle_ping(_: &Database, _: Vec<String>, buf: &mut Vec<u8>) {
    buf.write_all(b"+PONG\r\n").unwrap();
}
