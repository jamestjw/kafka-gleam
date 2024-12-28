import gleam/bytes_tree
import gleam/dict
import request.{type RequestHeader, Header}

type ErrorCode {
  NoError
  UnknownTopicOrPartition
  UnsupportedVersion
}

fn error_code_to_int(err) {
  case err {
    NoError -> 0
    UnknownTopicOrPartition -> 3
    UnsupportedVersion -> 35
  }
}

const empty_tagged_field_buffer = <<0:8>>

pub fn supported_apis() {
  dict.from_list([
    #(request.ApiVersions, #(0, 4)),
    #(request.DescribeTopicPartitions, #(0, 0)),
  ])
}

fn build_header(bytes, correlation_id) {
  // The size doesn't include the int of the size itself, hence the 4 here
  // is for the correlation ID
  let total_size = bytes_tree.byte_size(bytes) + 4
  bytes_tree.new()
  |> bytes_tree.append(<<total_size:size(32)>>)
  |> bytes_tree.append(<<correlation_id:size(32)>>)
  |> bytes_tree.append_tree(bytes)
}

// Returns the encoding in the form of a BitArray
fn encode_unsigned_varint(i) {
  case i >= 0 && i <= 0b1111111 {
    True -> <<i:size(8)>>
    // this is fine as the continuation bit is 0
    False -> todo as "actually encode this"
  }
}

fn append_error_code(bytes, err_code) {
  bytes
  |> bytes_tree.append(<<error_code_to_int(err_code):size(16)>>)
}

fn handle_api_versions_v3(correlation_id) {
  let append_supported_apis = fn(bytes) {
    let supported_apis = supported_apis()
    // As a compact array, this should be N+1
    let encoded_sz = encode_unsigned_varint(dict.size(supported_apis) + 1)

    bytes
    |> bytes_tree.append(encoded_sz)
    |> dict.fold(
      supported_apis,
      _,
      fn(bytes, api_key, versions) {
        let #(lower, upper) = versions
        bytes
        |> bytes_tree.append(<<request.api_key_to_int(api_key):size(16)>>)
        |> bytes_tree.append(<<lower:size(16)>>)
        |> bytes_tree.append(<<upper:size(16)>>)
        |> bytes_tree.append(empty_tagged_field_buffer)
      },
    )
  }

  bytes_tree.new()
  |> append_error_code(NoError)
  |> append_supported_apis()
  // throttle time ms
  |> bytes_tree.append(<<0:size(32)>>)
  |> bytes_tree.append(empty_tagged_field_buffer)
  |> build_header(correlation_id)
}

fn handle_describe_topic_partitions_v0(correlation_id) {
  bytes_tree.new()
  |> append_error_code(UnknownTopicOrPartition)
  |> build_header(correlation_id)
}

pub fn build_unsupported_version_resp(correlation_id) {
  bytes_tree.new()
  |> bytes_tree.append(<<10:size(32)>>)
  |> bytes_tree.append(<<correlation_id:size(32)>>)
  |> bytes_tree.append(<<error_code_to_int(UnsupportedVersion):size(16)>>)
}

pub fn process_request(header: RequestHeader) {
  case header.request_api_key {
    request.ApiVersions -> handle_api_versions_v3(header.correlation_id)
    request.DescribeTopicPartitions ->
      handle_describe_topic_partitions_v0(header.correlation_id)
    // _ -> panic as "not implemented yet"
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
