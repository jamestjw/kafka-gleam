import gleam/bytes_builder
import gleam/dict
import gleam/erlang/process
import gleam/io
import gleam/option.{None}
import gleam/otp/actor
import glisten.{Packet}
import parser

type ErrorCode {
  UnsupportedVersion
}

fn error_code_to_int(err) {
  case err {
    UnsupportedVersion -> 35
  }
}

const empty_tagged_field_buffer = <<0:8>>

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
      case header.request_api_key {
        parser.ApiVersions -> handle_api_versions_v3(header.correlation_id)
        _ -> panic as "not implemented yet"
      }
  }
}

fn build_header(correlation_id, body_size) {
  // The size doesn't include the int of the size itself, hence the 4 here
  // is for the correlation ID
  let total_size = body_size + 4
  bytes_builder.new()
  |> bytes_builder.append(<<total_size:size(32)>>)
  |> bytes_builder.append(<<correlation_id:size(32)>>)
}

// Returns the encoding in the form of a BitArray
fn encode_unsigned_varint(i) {
  case i >= 0 && i <= 0b1111111 {
    True -> <<i:size(8)>>
    // this is fine as the continuation bit is 0
    False -> todo as "actually encode this"
  }
}

fn handle_api_versions_v3(correlation_id) {
  let append_supported_apis = fn(bytes) {
    let supported_apis = parser.supported_apis()
    // As a compact array, this should be N+1
    let encoded_sz = encode_unsigned_varint(dict.size(supported_apis) + 1)

    bytes
    |> bytes_builder.append(encoded_sz)
    |> dict.fold(
      supported_apis,
      _,
      fn(bytes, api_key, versions) {
        let #(lower, upper) = versions
        bytes
        |> bytes_builder.append(<<parser.api_key_to_int(api_key):size(16)>>)
        |> bytes_builder.append(<<lower:size(16)>>)
        |> bytes_builder.append(<<upper:size(16)>>)
        |> bytes_builder.append(empty_tagged_field_buffer)
      },
    )
  }

  let body =
    bytes_builder.new()
    // no error
    |> bytes_builder.append(<<0:size(16)>>)
    |> append_supported_apis()
    // throttle time ms
    |> bytes_builder.append(<<0:size(32)>>)
    |> bytes_builder.append(empty_tagged_field_buffer)

  build_header(correlation_id, bytes_builder.byte_size(body))
  |> bytes_builder.append_builder(body)
}
