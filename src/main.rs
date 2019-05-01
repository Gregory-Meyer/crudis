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

mod connection;
mod database;
mod instance;
mod resp;

use database::Database;

use std::{env, iter, net::SocketAddr};

use tokio::{
    net::{TcpListener, TcpStream},
    prelude::*,
};

fn handle(dbs: &[Database], socket: TcpStream) {}

fn main() {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6379".to_string())
        .parse::<SocketAddr>()
        .expect("couldn't parse string as an address");

    let databases: Vec<_> = iter::repeat_with(|| Database::new()).take(16).collect();
    let socket = TcpListener::bind(&addr).expect("couldn't bind TCP listener");
    let incoming = socket.incoming();

    let server = incoming
        .map_err(|e| eprintln!("couldn't accept TCP connection: {}", e))
        .for_each(|socket| Ok(()));

    tokio::run(server);
}
