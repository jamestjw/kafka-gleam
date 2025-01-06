import gleam/bit_array
import gleam/bytes_tree
import gleam/dict
import gleam/io
import gleam/list
import gleam/result
import gleam/string
import request.{type RequestBody, type RequestHeader, Header}
import state.{type State}

type ErrorCode {
  NoError
  UnknownTopicOrPartition
  UnsupportedVersion
  UnknownTopic
}

fn error_code_to_int(err) {
  case err {
    NoError -> 0
    UnknownTopicOrPartition -> 3
    UnsupportedVersion -> 35
    UnknownTopic -> 100
  }
}

const empty_tagged_field_buffer = <<0:8>>

pub fn supported_apis() {
  dict.from_list([
    #(request.ApiVersions, #(0, 4)),
    #(request.DescribeTopicPartitions, #(0, 0)),
    #(request.Fetch, #(0, 16)),
  ])
}

fn build_response_header_v0(bytes, correlation_id) {
  // The size doesn't include the int of the size itself, hence the 4 here
  // is for the correlation ID
  let total_size = bytes_tree.byte_size(bytes) + 4
  bytes_tree.new()
  |> bytes_tree.append(<<total_size:size(32)>>)
  |> bytes_tree.append(<<correlation_id:size(32)>>)
  |> bytes_tree.append_tree(bytes)
}

// Turns out that different APIs expect different versions of the response
// header
fn build_response_header_v1(bytes, correlation_id) {
  // The size doesn't include the int of the size itself, hence the 5 here
  // is for the correlation ID and the tagged field
  let total_size = bytes_tree.byte_size(bytes) + 4 + 1
  bytes_tree.new()
  |> bytes_tree.append(<<total_size:size(32)>>)
  |> bytes_tree.append(<<correlation_id:size(32)>>)
  |> bytes_tree.append(empty_tagged_field_buffer)
  |> bytes_tree.append_tree(bytes)
}

// Returns the encoding in the form of a BitArray
pub fn encode_unsigned_varint(i) {
  case i >= 0 && i <= 0b1111111 {
    // MSB (i.e. the continuation bit) is already 0
    True -> <<i:size(8)>>
    False -> {
      // 128 = 2 ^ 7
      let rem = i % 128
      let rest = i / 128
      bit_array.append(<<1:1, rem:7>>, encode_unsigned_varint(rest))
    }
  }
}

fn append_error_code(bytes, err_code) {
  bytes
  |> bytes_tree.append(<<error_code_to_int(err_code):size(16)>>)
}

fn append_throttle_time(bytes, ms) {
  bytes
  |> bytes_tree.append(<<ms:size(32)>>)
}

fn handle_api_versions_v3(correlation_id) {
  let append_supported_apis = fn(bytes) {
    let supported_apis = supported_apis()
    // As a compact array, this should be N+1
    let encoded_sz = encode_unsigned_varint(dict.size(supported_apis) + 1)

    bytes
    |> bytes_tree.append(encoded_sz)
    |> dict.fold(supported_apis, _, fn(bytes, api_key, versions) {
      let #(lower, upper) = versions
      bytes
      |> bytes_tree.append(<<request.api_key_to_int(api_key):size(16)>>)
      |> bytes_tree.append(<<lower:size(16)>>)
      |> bytes_tree.append(<<upper:size(16)>>)
      |> bytes_tree.append(empty_tagged_field_buffer)
    })
  }

  bytes_tree.new()
  |> append_error_code(NoError)
  |> append_supported_apis()
  // throttle time ms
  |> append_throttle_time(0)
  |> bytes_tree.append(empty_tagged_field_buffer)
  |> build_response_header_v0(correlation_id)
}

fn append_compact_array(bytes, elems, append_fn) {
  bytes
  |> bytes_tree.append(encode_unsigned_varint(list.length(elems) + 1))
  |> list.fold(elems, _, append_fn)
}

fn append_n_bytes(bytes, i, n) {
  let num_bits = n * 8
  bytes_tree.append(bytes, <<i:size(num_bits)>>)
}

fn append_4_bytes(bytes, i) {
  append_n_bytes(bytes, i, 4)
}

// TODO: don't hardcode this
fn append_topic_authorized_operations(bytes) {
  bytes
  |> bytes_tree.append(<<0x00000df8:32>>)
}

fn append_topic_partitions(bytes, partitions: List(state.Partition)) {
  let append_partition = fn(bytes, partition) {
    let state.Partition(
      id:,
      leader_replica_id:,
      leader_epoch:,
      replica_ids:,
      in_sync_replica_ids:,
      ..,
    ) = partition
    bytes
    |> append_error_code(NoError)
    |> append_4_bytes(id)
    |> append_4_bytes(leader_replica_id)
    |> append_4_bytes(leader_epoch)
    |> append_compact_array(replica_ids, append_4_bytes)
    |> append_compact_array(in_sync_replica_ids, append_4_bytes)
    // TODO: eligible leader replicas
    |> append_compact_array([], append_4_bytes)
    // TODO: last known eligible leader replicas
    |> append_compact_array([], append_4_bytes)
    // TODO: offline replicas
    |> append_compact_array([], append_4_bytes)
    |> append_tag_buffer()
  }
  bytes
  |> append_unsigned_varint(list.length(partitions) + 1)
  |> list.fold(partitions, _, append_partition)
}

fn append_topic(bytes, state: State, topic) {
  case dict.get(state.topic_to_id, topic) {
    Ok(topic_uuid) -> {
      let partitions =
        dict.get(state.topic_uuid_to_partition, topic_uuid) |> result.unwrap([])
      bytes
      |> append_error_code(NoError)
      |> encode_compact_string(topic)
      // 16 bytes topic UUID
      |> bytes_tree.append(<<topic_uuid:128>>)
      // Is topic internal (boolean)
      |> encode_boolean(False)
      // Looks like order matters and it should be ascending
      |> append_topic_partitions(list.reverse(partitions))
      |> append_topic_authorized_operations()
      |> bytes_tree.append(empty_tagged_field_buffer)
    }
    Error(_) ->
      bytes
      |> append_error_code(UnknownTopicOrPartition)
      |> encode_compact_string(topic)
      // 16 bytes topic UUID
      |> bytes_tree.append(<<0:128>>)
      // Is topic internal (boolean)
      |> encode_boolean(False)
      // Empty partitions array
      |> append_unsigned_varint(1)
      |> append_topic_authorized_operations()
      |> bytes_tree.append(empty_tagged_field_buffer)
  }
}

fn handle_describe_topic_partitions_v0(state, correlation_id, topics) {
  bytes_tree.new()
  |> append_throttle_time(0)
  |> append_compact_array(topics, fn(bytes, topic) {
    append_topic(bytes, state, topic)
  })
  // Cursor for pagination, null for now
  |> append_null()
  |> append_tag_buffer()
  |> build_response_header_v1(correlation_id)
}

pub fn build_unsupported_version_resp(correlation_id) {
  bytes_tree.new()
  |> bytes_tree.append(<<10:size(32)>>)
  |> bytes_tree.append(<<correlation_id:size(32)>>)
  |> bytes_tree.append(<<error_code_to_int(UnsupportedVersion):size(16)>>)
}

fn handle_fetch(_state, correlation_id, body) {
  let append_partition = fn(bytes, partition: request.FetchTopicPartition) {
    bytes
    // partition index
    |> append_4_bytes(partition.id)
    // TODO: should this be zero?
    |> append_error_code(UnknownTopic)
    // high watermark
    |> append_n_bytes(0, 8)
    // last_stable_offset
    |> append_n_bytes(0, 8)
    // log_start_offset
    |> append_n_bytes(0, 8)
    // aborted_transactions => producer_id first_offset TAG_BUFFER
    |> append_compact_array([], fn(bytes, _) { bytes })
    // preferred_read_replica => INT32
    |> append_n_bytes(0, 4)
    // records => COMPACT_RECORDS
    // TODO: whats this?
    |> append_compact_array([], fn(bytes, _) { bytes })
    |> append_tag_buffer()
  }
  let append_topic = fn(bytes, topic) {
    let #(topic_id, partitions) = topic
    bytes
    |> append_n_bytes(topic_id, 16)
    |> append_compact_array(partitions, append_partition)
    |> append_tag_buffer()
  }

  let assert request.FetchBody(session_id:, topics:, ..) = body
  bytes_tree.new()
  |> append_throttle_time(0)
  |> append_error_code(NoError)
  |> append_4_bytes(session_id)
  |> append_compact_array(topics, append_topic)
  |> append_tag_buffer()
  |> build_response_header_v1(correlation_id)
}

pub fn process_request(state: State, header: RequestHeader, body: RequestBody) {
  case body {
    request.ApiVersionsBody -> handle_api_versions_v3(header.correlation_id)
    request.DescribeTopicPartitionsBody(topics, _) ->
      handle_describe_topic_partitions_v0(state, header.correlation_id, topics)
    request.FetchBody(..) -> handle_fetch(state, header.correlation_id, body)
  }
}

fn is_api_supported(key, version) {
  case dict.get(supported_apis(), key) {
    Error(_) -> False
    Ok(#(low, high)) -> low <= version && version <= high
  }
}

pub fn validate_header_api_version(header) {
  let Header(_, request_api_key, request_api_version, _, _) = header
  is_api_supported(request_api_key, request_api_version)
}

fn encode_compact_string(bytes, str) {
  bytes
  |> bytes_tree.append(encode_unsigned_varint(string.length(str) + 1))
  |> bytes_tree.append_string(str)
}

fn encode_boolean(bytes, bool) {
  case bool {
    True ->
      bytes
      |> bytes_tree.append(<<1:8>>)
    False ->
      bytes
      |> bytes_tree.append(<<0:8>>)
  }
}

fn append_unsigned_varint(bytes, i) {
  bytes |> bytes_tree.append(encode_unsigned_varint(i))
}

fn append_null(bytes) {
  bytes |> bytes_tree.append(<<0xff:8>>)
}

fn append_tag_buffer(bytes) {
  bytes
  |> bytes_tree.append(empty_tagged_field_buffer)
}
