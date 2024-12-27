import gleam/io
import parser

// import gleam/bit_array
import gleam/bytes_builder
import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor
import glisten.{Packet}

type ErrorCode {
  UnsupportedVersion
}

fn error_code_to_int(err) {
  case err {
    UnsupportedVersion -> 35
  }
}

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
  case parser.validate_header_api_version(header) {
    False ->
      bytes_builder.new()
      |> bytes_builder.append(<<10:size(32)>>)
      |> bytes_builder.append(<<header.correlation_id:size(32)>>)
      |> bytes_builder.append(<<error_code_to_int(UnsupportedVersion):size(16)>>)
    True ->
      bytes_builder.new()
      |> bytes_builder.append(<<8:size(32)>>)
      |> bytes_builder.append(<<header.correlation_id:size(32)>>)
  }
}
