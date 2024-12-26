import gleam/io

import gleam/bytes_builder
import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor
import glisten

pub fn main() {
  // Ensures gleam doesn't complain about unused imports in stage 1 (feel free to remove this!)
  let _ = glisten.handler
  let _ = glisten.serve
  let _ = process.sleep_forever
  let _ = actor.continue
  let _ = None

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      io.println("received connection")
      let resp = handle_request(msg)
      let assert Ok(_) = glisten.send(conn, resp)
      actor.continue(state)
    })
    |> glisten.serve(9092)

  process.sleep_forever()
}

fn handle_request(_msg) {
  let correlation_id = 7
  bytes_builder.new()
  |> bytes_builder.append(<<8:size(32)>>)
  |> bytes_builder.append(<<correlation_id:size(32)>>)
}
