import gleam/bytes_builder
import gleam/int
import gleam/string

pub fn to_hex(bits: BitArray) -> String {
  case bits |> bytes_builder.from_bit_array |> bytes_builder.to_bit_array {
    <<>> -> ""
    <<byte:8, rest:bits>> -> {
      let hex = int.to_base16(byte)
      // Ensure each byte is represented by two hex digits
      let padded_hex = case string.length(hex) {
        1 -> "0" <> hex
        _ -> hex
      }
      padded_hex <> " " <> to_hex(rest)
    }
    _ -> panic as "should be impossible"
  }
}
