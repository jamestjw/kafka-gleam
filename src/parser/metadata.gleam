import gleam/result
import parser/internal.{
  parse_array, parse_byte, parse_compact_array, parse_compact_string,
  parse_n_bytes, parse_signed_varint, parse_unsigned_varint,
}
import utils.{result_try_with_err}

// This module provides a means of parsing a Kafka metadata log file
pub type RecordValue {
  TopicValue(name: String, uuid: Int)
  PartitionValue(
    partition_id: Int,
    topic_uuid: Int,
    replica_ids: List(Int),
    in_sync_replica_ids: List(Int),
    removing_replica_ids: List(Int),
    adding_replica_ids: List(Int),
    leader_replica_id: Int,
    leader_epoch: Int,
    partition_epoch: Int,
    directory_uuids: List(Int),
  )
  FeatureLevelValue(name: String, level: Int)
}

pub type RecordValueType {
  Topic
  Partition
  FeatureLevel
}

pub type Record {
  Record(
    length: Int,
    attrs: Int,
    timestamp_delta: Int,
    offset_delta: Int,
    key_length: Int,
    key: Nil,
    value_length: Int,
    frame_version: Int,
    record_version: Int,
    value: RecordValue,
    headers_length: Int,
  )
}

pub type ParseMetadataError {
  MalformedRecord
  MalformedRecordValue
  InvalidRecordValueType
  MalformedRecordBatch
}

pub type RecordBatch {
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

fn record_value_type_of_int(i) {
  case i {
    2 -> Ok(Topic)
    3 -> Ok(Partition)
    12 -> Ok(FeatureLevel)
    _ -> Error(Nil)
  }
}

fn res_int_pred(res) {
  case res {
    Ok(#(i, bytes)) -> Ok(#(i - 1, bytes))
    Error(e) -> Error(e)
  }
}

fn parse_record_value(bytes) {
  let try_with_error = fn(res: Result(a, b), cb) {
    result_try_with_err(res, MalformedRecordValue, cb)
  }
  use #(frame_version, bytes) <- try_with_error(parse_byte(bytes))
  use #(value_type, bytes) <- try_with_error(parse_byte(bytes))
  use value_type <- result_try_with_err(
    record_value_type_of_int(value_type),
    InvalidRecordValueType,
  )
  use #(version, bytes) <- try_with_error(parse_byte(bytes))
  case value_type {
    FeatureLevel -> {
      use #(name, bytes) <- try_with_error(parse_compact_string(bytes))
      use #(level, bytes) <- try_with_error(parse_n_bytes(bytes, 2))
      use #(_tagged_fields_count, bytes) <- try_with_error(parse_byte(bytes))
      Ok(#(frame_version, version, FeatureLevelValue(name:, level:), bytes))
    }
    Topic -> {
      use #(name, bytes) <- try_with_error(parse_compact_string(bytes))
      use #(uuid, bytes) <- try_with_error(parse_n_bytes(bytes, 16))
      use #(_tagged_fields_count, bytes) <- try_with_error(parse_byte(bytes))
      Ok(#(frame_version, version, TopicValue(name:, uuid:), bytes))
    }
    Partition -> {
      use #(partition_id, bytes) <- try_with_error(parse_n_bytes(bytes, 4))
      use #(topic_uuid, bytes) <- try_with_error(parse_n_bytes(bytes, 16))
      use #(replicas_len, bytes) <- try_with_error(
        parse_unsigned_varint(bytes)
        |> res_int_pred,
      )
      use #(replica_ids, bytes) <- try_with_error(
        parse_array(bytes, replicas_len, parse_n_bytes(_, 4)),
      )
      use #(in_sync_replica_ids, bytes) <- try_with_error(
        parse_compact_array(bytes, parse_n_bytes(_, 4)),
      )
      use #(removing_replica_ids, bytes) <- try_with_error(
        parse_compact_array(bytes, parse_n_bytes(_, 4)),
      )
      use #(adding_replica_ids, bytes) <- try_with_error(
        parse_compact_array(bytes, parse_n_bytes(_, 4)),
      )
      use #(leader_replica_id, bytes) <- try_with_error(parse_n_bytes(bytes, 4))
      use #(leader_epoch, bytes) <- try_with_error(parse_n_bytes(bytes, 4))
      use #(partition_epoch, bytes) <- try_with_error(parse_n_bytes(bytes, 4))
      use #(directory_uuids, bytes) <- try_with_error(
        parse_compact_array(bytes, parse_n_bytes(_, 16)),
      )
      use #(_tagged_fields_count, bytes) <- try_with_error(parse_byte(bytes))
      Ok(#(
        frame_version,
        version,
        PartitionValue(
          partition_id:,
          topic_uuid:,
          replica_ids:,
          in_sync_replica_ids:,
          removing_replica_ids:,
          adding_replica_ids:,
          leader_replica_id:,
          leader_epoch:,
          partition_epoch:,
          directory_uuids:,
        ),
        bytes,
      ))
    }
  }
}

fn parse_record(bytes) {
  let try_with_error = fn(res, cb) {
    result_try_with_err(res, MalformedRecord, cb)
  }
  use #(record_length, bytes) <- try_with_error(parse_signed_varint(bytes))
  use #(attrs, bytes) <- try_with_error(parse_byte(bytes))
  use #(timestamp_delta, bytes) <- try_with_error(parse_signed_varint(bytes))
  use #(offset_delta, bytes) <- try_with_error(parse_signed_varint(bytes))
  use #(key_length, bytes) <- try_with_error(parse_signed_varint(bytes))
  // TODO: Handle parsing of key
  let assert -1 = key_length
  use #(value_length, bytes) <- try_with_error(parse_signed_varint(bytes))
  use #(frame_version, record_version, record_value, bytes) <- result.try(
    parse_record_value(bytes),
  )
  use #(num_headers, bytes) <- try_with_error(parse_unsigned_varint(bytes))
  // TODO: For now, assume that we don't have any headers
  let assert 0 = num_headers
  Ok(#(
    Record(
      length: record_length,
      attrs:,
      timestamp_delta:,
      offset_delta:,
      key_length:,
      key: Nil,
      value_length:,
      frame_version:,
      record_version:,
      value: record_value,
      headers_length: num_headers,
    ),
    bytes,
  ))
}

pub fn parse_record_batch(bytes) {
  case bytes {
    <<
      base_offset:signed-64,
      batch_length:signed-32,
      partition_leader_epoch:signed-32,
      magic_bytes:signed-8,
      crc:signed-32,
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
      use #(records, rest) <- result.try(parse_array(
        rest,
        records_length,
        parse_record,
      ))
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
    _ -> Error(MalformedRecordBatch)
  }
}

pub fn parse_record_batches(
  bytes,
) -> Result(List(RecordBatch), ParseMetadataError) {
  use #(record_batch, bytes) <- result.try(parse_record_batch(bytes))
  case bytes {
    <<>> -> Ok([record_batch])
    bytes -> {
      use record_batches <- result.try(parse_record_batches(bytes))
      Ok([record_batch, ..record_batches])
    }
  }
}
