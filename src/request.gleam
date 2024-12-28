import gleam/option.{None, Some}

pub type RequestHeader {
  Header(
    message_size: Int,
    request_api_key: ApiKey,
    request_api_version: Int,
    correlation_id: Int,
  )
}

pub type ApiKey {
  ApiVersions
  DescribeTopicPartitions
}

// TODO: implement the same function that does the conversion the other way
pub fn api_key_from_int(i) {
  case i {
    18 -> Some(ApiVersions)
    75 -> Some(DescribeTopicPartitions)
    _ -> None
  }
}

pub fn api_key_to_int(api_key) {
  case api_key {
    ApiVersions -> 18
    DescribeTopicPartitions -> 75
  }
}
