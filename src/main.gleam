import gleam/erlang/process
import gleam/io
import gleam/option.{None}
import gleam/otp/actor
import glisten.{Packet}
import parser
import server
import utils

pub fn main() {
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      let resp = handle_request(msg)
      let assert Ok(_) = glisten.send(conn, resp)
      actor.continue(state)
    })
    |> glisten.serve(9092)

  process.sleep_forever()
}

fn handle_request(msg) {
  let assert Packet(msg) = msg

  io.println("Received message:")
  io.debug(utils.bit_array_to_hex(msg))

  case parser.parse_msg(msg) {
    Ok(#(header, body)) -> server.process_request(header, body)
    Error(parser.UnsupportedApiKey(correlation_id)) ->
      server.build_unsupported_version_resp(correlation_id)
    Error(_) -> panic
  }
}
