import gleam/erlang/process
import gleam/io
import gleam/option.{None}
import gleam/otp/actor
import gleam/string
import glisten.{Packet}
import parser
import parser/metadata
import server
import simplifile
import state
import utils

const metadata_log_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

fn build_initial_state() {
  let empty_state = state.new()
  case simplifile.read_bits(metadata_log_path) {
    Error(e) -> {
      io.println_error(
        "Couldn't read file ("
        <> metadata_log_path
        <> "): "
        <> simplifile.describe_error(e),
      )
      empty_state
    }
    Ok(bytes) -> {
      case metadata.parse_record_batches(bytes) {
        Error(e) -> {
          io.println_error(
            "Couldn't parse metadata log file: " <> string.inspect(e),
          )
          empty_state
        }
        Ok(record_batches) -> {
          empty_state |> state.load_record_batches(record_batches)
        }
      }
    }
  }
}

pub fn main() {
  let assert Ok(_) =
    glisten.handler(
      fn(_conn) { #(build_initial_state(), None) },
      fn(msg, state, conn) {
        let resp = handle_request(state, msg)
        let assert Ok(_) = glisten.send(conn, resp)
        actor.continue(state)
      },
    )
    |> glisten.serve(9092)

  process.sleep_forever()
}

fn handle_request(state, msg) {
  let assert Packet(msg) = msg

  io.println("Received message:")
  io.debug(utils.bit_array_to_hex(msg))

  case parser.parse_msg(msg) {
    Ok(#(header, body)) -> server.process_request(state, header, body)
    Error(parser.UnsupportedApiKey(correlation_id)) ->
      server.build_unsupported_version_resp(correlation_id)
    Error(e) -> {
      panic as string.inspect(e)
    }
  }
}
