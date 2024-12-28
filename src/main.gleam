import gleam/erlang/process
import gleam/io
import gleam/option.{None}
import gleam/otp/actor
import glisten.{Packet}
import parser
import server

pub fn main() {
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

fn handle_request(msg) {
  let assert Packet(msg) = msg
  let assert Ok(header) = parser.parse_msg(msg)
  case server.validate_header_api_version(header) {
    False -> server.build_unsupported_version_resp(header.correlation_id)
    True -> server.process_request(header)
  }
}
