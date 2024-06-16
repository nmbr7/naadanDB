use std::io::{Cursor, Read, Write};

use crate::utils::log;

pub(crate) fn write_string_to_buf(
    dyn_cursor_base: &u64,
    last_dyn_offset: &mut u64,
    buf_cursor: &mut Cursor<&mut Vec<u8>>,
    str_val: &String,
) {
    // TODO: If this is an update then check if the existing space is enough or we need to allocate new space
    //       will have to add a free list metadata in header or, the a background task will have to occationaly
    //       read the dynamic memory and rebalance the space.
    let offset_index = last_dyn_offset.to_be_bytes();

    buf_cursor.write_all(&offset_index).unwrap();
    let current_pos = buf_cursor.position();

    // Seek to dynamic content region last entry offset.
    log(
        "Utils".to_string(),
        format!(
            "Current pos {}, String write offset {} and string is {}",
            buf_cursor.position(),
            *last_dyn_offset,
            str_val
        ),
    );
    buf_cursor.set_position(*dyn_cursor_base + *last_dyn_offset);
    log(
        "Utils".to_string(),
        format!("String write offset {}", *last_dyn_offset),
    );

    // write string length in 2 byte
    buf_cursor
        .write_all(&(str_val.len() as u16).to_be_bytes())
        .unwrap();

    // Write string content
    buf_cursor.write_all(str_val.as_bytes()).unwrap();
    buf_cursor.set_position(current_pos);

    // Update the dynamic content region last entry offset
    *last_dyn_offset += 2 + str_val.len() as u64;
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

    log(
        "Utils".to_string(),
        format!(
            "String read page offset is {}  - Fixed base is {}",
            buf_cursor.position(),
            current_pos
        ),
    );

    match buf_cursor.read_exact(&mut len_buf) {
        Ok(_) => {}
        Err(err) => {
            log("Utils".to_string(), format!("Error: {}", err));
            panic!()
        }
    }

    let len = u16::from_be_bytes(len_buf);

    let mut data_buf = vec![0; len as usize];

    buf_cursor.read_exact(&mut data_buf).unwrap();

    let string_value = String::from_utf8(data_buf).unwrap();

    if reset_offset {
        buf_cursor.set_position(current_pos);
    }

    string_value
}
