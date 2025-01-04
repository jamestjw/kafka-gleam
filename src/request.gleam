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
