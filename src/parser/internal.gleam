import gleam/bit_array
import utils

fn parse_varint_inner(bits, acc, parse_bits_fn) {
  case bits {
    <<cont:1, bits:7, rest:bits>> if cont == 0 -> {
      let varint = bit_array.append(<<bits:7>>, acc) |> parse_bits_fn()
      Ok(#(varint, rest))
    }
    <<cont:1, bits:7, rest:bits>> if cont == 1 -> {
      parse_varint_inner(rest, bit_array.append(<<bits:7>>, acc), parse_bits_fn)
    }
    _ -> Error(Nil)
  }
}

pub fn parse_unsigned_varint(bits) {
  parse_varint_inner(bits, <<>>, utils.bits_to_unsigned_int)
}

fn bits_to_signed_int(bits) {
  let n = utils.bits_to_unsigned_int(bits)
  // Undo the zig-zag pattern, where
  // Positive integers p are encoded as 2 * p (the even numbers), while negative
  // integers n are encoded as 2 * |n| - 1 (the odd numbers).
  case n % 2 == 0 {
    True -> n / 2
    False -> -1 * { n + 1 } / 2
  }
}

pub fn parse_signed_varint(bits) {
  parse_varint_inner(bits, <<>>, bits_to_signed_int)
}

pub fn parse_byte(bits) {
  case bits {
    <<byte:8, rest:bits>> -> Ok(#(byte, rest))
    _ -> Error(Nil)
  }
}
