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

use std::{env, net::SocketAddr};

use tokio::{
    io,
    net::tcp::{TcpListener, TcpStream},
    prelude::*,
};

use nom::Context;

struct CommandStream {
    stream: TcpStream,
    buf: Vec<u8>,
    has_failed: bool,
}

impl CommandStream {
    fn from_socket(stream: TcpStream) -> CommandStream {
        CommandStream {
            stream,
            buf: Vec::with_capacity(4096),
            has_failed: true,
        }
    }

    fn try_parse(&mut self) -> Option<Vec<String>> {
        match resp::parse_client_message(self.buf.as_slice()) {
            Ok((rest, msg)) => {
                let to_trim = self.buf.len() - rest.len();
                self.buf.drain(..to_trim);

                return Some(msg);
            }
            Err(e) => match e {
                nom::Err::Incomplete(_) => self.has_failed = true,
                nom::Err::Error(c) | nom::Err::Failure(c) => {
                    let last_parsed_idx = match c {
                        Context::Code(i, _) => self.buf.len() - i.len(),
                    };

                    match self
                        .buf
                        .iter()
                        .skip(last_parsed_idx)
                        .position(|b| *b == b'\n')
                    {
                        Some(i) => {
                            self.buf.drain(..i + 1);
                        }
                        None => self.buf.clear(),
                    }
                }
            },
        }

        None
    }
}

impl Stream for CommandStream {
    type Item = Vec<String>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut new_buf = [0; 4096];

        eprintln!(
            "[{:?}] poll: self.buf = {:?}",
            std::time::Instant::now(),
            self.buf
        );

        loop {
            if !self.has_failed {
                if let Some(msg) = self.try_parse() {
                    return Ok(Async::Ready(Some(msg)));
                }
            }

            let num_read = match self.stream.poll_read(&mut new_buf)? {
                Async::Ready(n) => n,
                Async::NotReady => return Ok(Async::NotReady),
            };

            if num_read == 0 {
                continue;
            }

            self.has_failed = false;
            self.buf.extend_from_slice(&new_buf[..num_read]);
        }
    }
}

fn main() {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "[::1]:6379".to_string())
        .parse::<SocketAddr>()
        .expect("couldn't parse string as an address");
    let listener = TcpListener::bind(&addr).expect("couldn't bind TCP listener");

    let server = listener
        .incoming()
        .map_err(|e| eprintln!("couldn't accept a TCP connection: {}", e))
        .for_each(|sock| {
            let stream = CommandStream::from_socket(sock);

            tokio::spawn(
                stream
                    .for_each(|msg| {
                        println!("received a message: '{:?}'", msg);
                        Ok(())
                    })
                    .map_err(|e| {
                        eprintln!("couldn't parse message: {}", e);
                    }),
            )
        });

    tokio::run(server);
}
