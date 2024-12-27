// import gleam/option.{None, Some}
import gleam/result

pub type ParseHeaderError {
  UnsupportedApiKey
  UnsupportedApiVersion
  MalformedHeader
}

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
}

// TODO: implement the same function that does the conversion the other way
fn api_key_from_int(i) {
  case i {
    18 -> Ok(ApiVersions)
    _ -> Error(UnsupportedApiKey)
  }
}

fn validate_version(key, version) {
  case key {
    ApiVersions -> 0 <= version && version <= 4
    // _ -> False
  }
}

pub fn validate_header_api_version(header) {
  let Header(_, request_api_key, request_api_version, _) = header
  validate_version(request_api_key, request_api_version)
}

pub fn parse_msg(msg: BitArray) -> Result(RequestHeader, ParseHeaderError) {
  case msg {
    <<
      message_size:32,
      request_api_key:16,
      request_api_version:16,
      correlation_id:32,
      _rest:bits,
    >> -> {
      use request_api_key <- result.try(api_key_from_int(request_api_key))

      // case validate_version(request_api_key, request_api_version) {
      //   False -> Error(UnsupportedApiVersion)
      //   True ->
      Ok(Header(
        message_size,
        request_api_key,
        request_api_version,
        correlation_id,
      ))
      // }
    }
    _ -> Error(MalformedHeader)
  }
}
