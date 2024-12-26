import gleam/io

// import gleam/bit_array
import gleam/bytes_builder
import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor
import glisten.{Packet}

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

pub type RequestHeader {
  Header(
    message_size: Int,
    request_api_key: Int,
    request_api_version: Int,
    correlation_id: Int,
  )
}

fn parse_msg(msg) -> Result(RequestHeader, String) {
  case msg {
    <<
      message_size:32,
      request_api_key:16,
      request_api_version:16,
      correlation_id:32,
      _rest:bits,
    >> ->
      Ok(Header(
        message_size,
        request_api_key,
        request_api_version,
        correlation_id,
      ))
    _ -> Error("bad response header")
  }
}

fn handle_request(msg) {
  let assert Packet(msg) = msg
  let assert Ok(header) = parse_msg(msg)
  bytes_builder.new()
  |> bytes_builder.append(<<8:size(32)>>)
  |> bytes_builder.append(<<header.correlation_id:size(32)>>)
}
