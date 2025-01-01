import gleeunit/should
import parser/internal.{parse_unsigned_varint}
import server.{encode_unsigned_varint}

pub fn encode_unsigned_varint_roundtrip_test() {
  let input = 456_784_247_891_377

  encode_unsigned_varint(input)
  |> parse_unsigned_varint()
  |> should.be_ok()
  |> should.equal(#(input, <<>>))
}
