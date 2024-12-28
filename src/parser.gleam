import gleam/option
import gleam/result
import request.{type RequestHeader, Header}

pub type ParseHeaderError {
  MalformedHeader
  UnsupportedApiKey
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
      use request_api_key <- result.try(
        request.api_key_from_int(request_api_key)
        |> option.to_result(UnsupportedApiKey),
      )

      Ok(Header(
        message_size,
        request_api_key,
        request_api_version,
        correlation_id,
      ))
    }
    _ -> Error(MalformedHeader)
  }
}
