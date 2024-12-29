import gleam/list
import gleam/result
import parser/internal.{parse_byte, parse_signed_varint}
import utils.{result_swap_error}

// This module provides a means of parsing a Kafka metadata log file

type Record {
  Record
}

type ParseMetadataError {
  ParseRecordError
  MalformedRecordBatchError
}

type RecordBatch {
  RecordBatch(
    base_offset: Int,
    batch_length: Int,
    partition_leader_epoch: Int,
    magic_bytes: Int,
    crc: Int,
    attrs: Int,
    last_offset_delta: Int,
    base_timestamp: Int,
    max_timestamp: Int,
    producer_id: Int,
    producer_epoch: Int,
    base_sequence: Int,
    records_length: Int,
    records: List(Record),
  )
}

fn parse_record(bytes) {
  let res = {
    use #(records_length, rest) <- result.try(parse_signed_varint(bytes))
    use #(attrs, rest) <- result.try(parse_byte(rest))
    use #(timestamp_delta, rest) <- result.try(parse_signed_varint(bytes))
    use #(offset_delta, rest) <- result.try(parse_signed_varint(bytes))
    use #(key_length, rest) <- result.try(parse_signed_varint(bytes))
    // TODO: Handle parsing of key
    let assert -1 = key_length
    use #(value_length, rest) <- result.try(parse_signed_varint(bytes))
    todo as "parse the value"
  }

  result_swap_error(res, ParseRecordError)
}

fn parse_records(bytes, n, acc) {
  case n {
    0 -> Ok(#(list.reverse(acc), bytes))
    n if n < 0 -> panic as "n is an unsigned integer"
    n -> {
      use #(record, bytes) <- result.try(parse_record(bytes))
      parse_records(bytes, n - 1, [record, ..acc])
    }
  }
}

fn parse_record_batch(bytes) {
  case bytes {
    <<
      base_offset:64,
      batch_length:32,
      partition_leader_epoch:32,
      magic_bytes:8,
      crc:32,
      attrs:16,
      last_offset_delta:32,
      base_timestamp:64,
      max_timestamp:64,
      producer_id:signed-size(64),
      producer_epoch:signed-size(16),
      base_sequence:signed-size(32),
      records_length:32,
      rest:bits,
    >> -> {
      use #(records, rest) <- result.try(
        parse_records(rest, records_length, []),
      )
      Ok(#(
        RecordBatch(
          base_offset:,
          batch_length:,
          partition_leader_epoch:,
          magic_bytes:,
          crc:,
          attrs:,
          last_offset_delta:,
          base_timestamp:,
          max_timestamp:,
          producer_id:,
          producer_epoch:,
          base_sequence:,
          records_length:,
          records:,
        ),
        rest,
      ))
    }
    _ -> Error(MalformedRecordBatchError)
  }
}
