import gleam/bit_array
import gleam/list
import gleam/result
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

pub fn parse_n_bytes(bits, n) {
  let num_bits = n * 8
  case bits {
    <<byte:size(num_bits), rest:bits>> -> Ok(#(byte, rest))
    _ -> Error(Nil)
  }
}

pub fn bit_array_split(bits, n) {
  let num_bytes = bit_array.byte_size(bits)
  use left <- result.try(bit_array.slice(bits, 0, n))
  use right <- result.try(bit_array.slice(bits, n, num_bytes - n))
  Ok(#(left, right))
}

pub fn parse_compact_string(bits) {
  use #(str_len, rest) <- result.try(
    parse_unsigned_varint(bits) |> result.map(fn(x) { #(x.0 - 1, x.1) }),
  )
  let assert True = str_len >= 0
  use #(str_bits, rest) <- result.try(bit_array_split(rest, str_len))
  use str <- result.try(bit_array.to_string(str_bits))
  Ok(#(str, rest))
}

fn parse_array_helper(bytes, len, parse_elem, acc) {
  case len {
    0 -> Ok(#(list.reverse(acc), bytes))
    -1 -> Ok(#([], bytes))
    n if n < 0 -> {
      panic as "n should not be negative!"
    }
    n -> {
      use #(record, bytes) <- result.try(parse_elem(bytes))
      parse_array_helper(bytes, n - 1, parse_elem, [record, ..acc])
    }
  }
}

pub fn parse_array(bytes, len, parse_elem) {
  parse_array_helper(bytes, len, parse_elem, [])
}

pub fn res_int_pred(res) {
  case res {
    Ok(#(i, bytes)) -> Ok(#(i - 1, bytes))
    Error(e) -> Error(e)
  }
}

pub fn parse_compact_array(bytes, parse_elem) {
  use #(array_len, bytes) <- result.try(parse_unsigned_varint(bytes))
  parse_array(bytes, array_len - 1, parse_elem)
}
