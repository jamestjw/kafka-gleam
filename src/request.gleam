import gleam/option.{type Option, None, Some}

pub type RequestHeader {
  Header(
    message_size: Int,
    request_api_key: ApiKey,
    request_api_version: Int,
    correlation_id: Int,
    client_id: Option(String),
  )
}

pub type RequestBody {
  ApiVersionsBody
  DescribeTopicPartitionsBody(
    topics: List(String),
    response_partition_limit: Int,
  )
  FetchBody(
    max_wait_ms: Int,
    min_bytes: Int,
    max_bytes: Int,
    isolation_level: Int,
    session_id: Int,
    session_epoch: Int,
    // List of tuples of topic_id * partitions
    topics: List(#(Int, List(FetchTopicPartition))),
    // Tuple of topic_id, partition_ids
    forgotten_topics: List(#(Int, List(Int))),
    rack_id: String,
  )
}

pub type FetchTopicPartition {
  FetchTopicPartition(
    id: Int,
    current_leader_epoch: Int,
    fetch_offset: Int,
    last_fetched_epoch: Int,
    log_start_offset: Int,
    partition_max_bytes: Int,
  )
}

pub type ApiKey {
  ApiVersions
  Fetch
  DescribeTopicPartitions
}

// TODO: implement the same function that does the conversion the other way
pub fn api_key_from_int(i) {
  case i {
    1 -> Some(Fetch)
    18 -> Some(ApiVersions)
    75 -> Some(DescribeTopicPartitions)
    _ -> None
  }
}

pub fn api_key_to_int(api_key) {
  case api_key {
    Fetch -> 1
    ApiVersions -> 18
    DescribeTopicPartitions -> 75
  }
}
