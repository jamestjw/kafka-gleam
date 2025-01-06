import gleam/bit_array
import gleam/io
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import gleam/string
import parser/internal.{
  bit_array_split, parse_byte, parse_compact_array, parse_compact_string,
  parse_n_bytes, parse_unsigned_varint,
}
import request.{type RequestBody, type RequestHeader, Header}
import server

pub type ParseError {
  MalformedHeader
  // Gives us a way to return the correlation ID
  UnsupportedApiKey(Int)
  MalformedBody(String)
}

pub fn parse_msg(
  msg: BitArray,
) -> Result(#(RequestHeader, RequestBody), ParseError) {
  use #(header, rest) <- result.try(parse_header(msg))
  io.debug(header)

  use body <- result.try(case server.validate_header_api_version(header) {
    False -> Error(UnsupportedApiKey(header.correlation_id))
    True -> parse_body(header, rest)
  })

  io.debug(body)
  Ok(#(header, body))
}

fn parse_header(msg: BitArray) -> Result(#(RequestHeader, BitArray), ParseError) {
  let parse_client_id = fn(len, bits) {
    case len {
      -1 -> Ok(#(None, bits))
      len if len > 0 -> {
        use #(client_id_bits, rest) <- result.try(bit_array_split(bits, len))
        use client_id <- result.try(bit_array.to_string(client_id_bits))
        Ok(#(Some(client_id), rest))
      }
      _ -> Error(Nil)
    }
  }

  case msg {
    <<
      message_size:32,
      request_api_key:16,
      request_api_version:16,
      correlation_id:32,
      client_id_length:signed-size(16),
      rest:bits,
    >> -> {
      use request_api_key <- result.try(
        request.api_key_from_int(request_api_key)
        |> option.to_result(UnsupportedApiKey(correlation_id)),
      )

      use #(client_id, rest) <- result.try(
        parse_client_id(client_id_length, rest)
        |> result.map_error(fn(_) { MalformedHeader }),
      )

      // TODO: check that this is really zero?
      use #(_tag_buffer, rest) <- result.try(
        bit_array_split(rest, 1)
        |> result.map_error(fn(_) { MalformedHeader }),
      )

      Ok(#(
        Header(
          message_size:,
          request_api_key:,
          request_api_version:,
          correlation_id:,
          client_id:,
        ),
        rest,
      ))
    }
    _ -> Error(MalformedHeader)
  }
}

fn parse_body(header: RequestHeader, bits: BitArray) {
  case header.request_api_key {
    request.ApiVersions -> Ok(request.ApiVersionsBody)
    request.DescribeTopicPartitions ->
      parse_describe_topic_partitions_req(bits)
      |> result.map_error(fn(_) {
        MalformedBody("bad DescribeTopicPartitions body")
      })
    request.Fetch ->
      parse_fetch_req(bits)
      |> result.map_error(fn(_) { MalformedBody("bad Fetch body") })
  }
}

fn parse_topic(bits) {
  use #(topic_name, rest) <- result.try(parse_compact_string(bits))
  use #(_tag_buffer, rest) <- result.try(bit_array_split(rest, 1))
  Ok(#(topic_name, rest))
}

fn parse_topics(bits, n, acc) {
  case n {
    0 -> Ok(#(list.reverse(acc), bits))
    n if n < 0 -> panic as "shouldn't happen"
    n -> {
      use #(topic, rest) <- result.try(parse_topic(bits))
      parse_topics(rest, n - 1, [topic, ..acc])
    }
  }
}

fn parse_describe_topic_partitions_req(body: BitArray) {
  use #(n, rest) <- result.try(parse_unsigned_varint(body))
  let num_topics = n - 1
  use #(topics, rest) <- result.try(parse_topics(rest, num_topics, []))
  case rest {
    <<response_partition_limit:32, _cursor:8, _rest:bits>> ->
      Ok(request.DescribeTopicPartitionsBody(topics:, response_partition_limit:))
    _ -> Error(Nil)
  }
}

// Parse Fetch START

fn parse_fetch_forgotten_topic_data(bits) {
  use #(topic_uuid, bits) <- result.try(parse_n_bytes(bits, 16))
  use #(partition_ids, bits) <- result.try(
    parse_compact_array(bits, parse_n_bytes(_, 4)),
  )
  use #(_tag_buffer, bits) <- result.try(bit_array_split(bits, 1))
  Ok(#(#(topic_uuid, partition_ids), bits))
}

fn parse_fetch_topic_partition(bits) {
  case bits {
    <<
      id:32,
      current_leader_epoch:32,
      fetch_offset:64,
      last_fetched_epoch:32,
      log_start_offset:64,
      partition_max_bytes:32,
      _tag_buffer:8,
      rest:bits,
    >> ->
      Ok(#(
        request.FetchTopicPartition(
          id:,
          current_leader_epoch:,
          fetch_offset:,
          last_fetched_epoch:,
          log_start_offset:,
          partition_max_bytes:,
        ),
        rest,
      ))
    _ -> Error(Nil)
  }
}

fn parse_fetch_topic(bits) {
  use #(topic_uuid, bits) <- result.try(parse_n_bytes(bits, 16))
  use #(partitions, bits) <- result.try(parse_compact_array(
    bits,
    parse_fetch_topic_partition,
  ))
  use #(_tag_buffer, bits) <- result.try(bit_array_split(bits, 1))
  Ok(#(#(topic_uuid, partitions), bits))
}

fn parse_fetch_req(bits: BitArray) {
  case bits {
    <<
      max_wait_ms:32,
      min_bytes:32,
      max_bytes:32,
      isolation_level:8,
      session_id:32,
      session_epoch:32,
      bits:bits,
    >> -> {
      use #(topics, bits) <- result.try(parse_compact_array(
        bits,
        parse_fetch_topic,
      ))
      use #(forgotten_topics, bits) <- result.try(parse_compact_array(
        bits,
        parse_fetch_forgotten_topic_data,
      ))
      use #(rack_id, bits) <- result.try(parse_compact_string(bits))
      case parse_byte(bits) {
        Ok(#(_tag_buffer, <<>>)) ->
          Ok(request.FetchBody(
            max_wait_ms:,
            min_bytes:,
            max_bytes:,
            isolation_level:,
            session_id:,
            session_epoch:,
            topics:,
            forgotten_topics:,
            rack_id:,
          ))
        e -> {
          io.println("trailing bits? " <> string.inspect(e))
          Error(Nil)
        }
      }
    }

    _ -> Error(Nil)
  }
}
// Parse Fetch END
