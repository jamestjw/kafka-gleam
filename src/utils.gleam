import gleam/result
import gleam/bit_array
import gleam/bytes_tree
import gleam/int
import gleam/string

pub fn bytes_tree_to_hex(bt) -> String {
  bytes_tree.to_bit_array(bt) |> bit_array_to_hex()
}

pub fn bit_array_to_hex(bits: BitArray) -> String {
  case bits |> bytes_tree.from_bit_array |> bytes_tree.to_bit_array {
    <<>> -> ""
    <<byte:8, rest:bits>> -> {
      let hex = int.to_base16(byte)
      // Ensure each byte is represented by two hex digits
      let padded_hex = case string.length(hex) {
        1 -> "0" <> hex
        _ -> hex
      }
      padded_hex <> " " <> bit_array_to_hex(rest)
    }
    _ -> panic as "should be impossible"
  }
}

pub fn bits_to_unsigned_int(bits) {
  let len = bit_array.bit_size(bits)
  case bits {
    <<bits:unsigned-size(len)>> -> bits
    _ -> panic as "impossible"
  }
}

pub fn result_swap_error(res, err) {
  result.map_error(res, fn(_) { err })
}
