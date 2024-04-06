use std::io::{Cursor, Read, Write};

pub(crate) fn write_string_to_buf(
    dyn_cursor_base: &mut u64,
    buf_cursor: &mut Cursor<&mut Vec<u8>>,
    str_val: &String,
) {
    let offset = dyn_cursor_base.to_be_bytes();

    buf_cursor.write_all(&offset).unwrap();
    let current_pos = buf_cursor.position();

    // Seek to table column list id offset start location at 1024 * 2 bytes
    buf_cursor.set_position(*dyn_cursor_base as u64);

    // write table name length (max is 128) in 2 byte
    buf_cursor
        .write_all(&(str_val.len() as u16).to_be_bytes())
        .unwrap();

    // Write table name (max len is 128)
    buf_cursor.write_all(str_val.as_bytes()).unwrap();
    buf_cursor.set_position(current_pos);
    *dyn_cursor_base += str_val.len() as u64 + 2;
}

/// Read string from a byte slice (Format: |StringLen|StringBytes|)
pub(crate) fn read_string_from_buf(
    buf_cursor: &mut Cursor<&Vec<u8>>,
    offset: u64,
    reset_offset: bool,
) -> String {
    let mut len_buf = [0u8; 2];
    let current_pos = buf_cursor.position();
    buf_cursor.set_position(offset);

    buf_cursor.read_exact(&mut len_buf).unwrap();

    let len = u16::from_be_bytes(len_buf);

    let mut data_buf = vec![0; len as usize];

    buf_cursor.read_exact(&mut data_buf).unwrap();

    let table_name = String::from_utf8(data_buf).unwrap();

    if reset_offset {
        buf_cursor.set_position(current_pos);
    }

    table_name
}
