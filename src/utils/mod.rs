use std::{fs::File, io::Write};

use log::debug;

const LOG_CHAR_LIMIT: usize = usize::pow(2, 10);

pub fn log(scope: String, mut string: String) {
    if string.len() > LOG_CHAR_LIMIT {
        string.truncate(LOG_CHAR_LIMIT);
        string += "......."
    }

    let log_string = format!(
        "\x1b[32m{}\x1b[38;5;212m{}\x1b[m {}",
        "[DEBUG]",
        format!("({}) -", scope),
        string
    );

    //println!("{}", log_string);

    let file_log_string = format!("{}{} {}", "[DEBUG]", format!("({}) -", scope), string);
    let mut file = File::options()
        .create(true)
        .write(true)
        .append(true)
        .open("/tmp/Naadan_db.log")
        .unwrap();

    // file.write_all(format!("{}\n", file_log_string).as_bytes())
    //     .unwrap();
    // file.flush().unwrap();
}

#[macro_export]
macro_rules! rc_ref_cell {
    ($a:expr) => {
        Rc::new(RefCell::new($a))
    };
}
