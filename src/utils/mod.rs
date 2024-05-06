use log::debug;

pub fn log(scope: String, string: String) {
    println!("\x1b[32m[DEBUG] \x1b[38;5;212m({scope}) -\x1b[m {}", string);
}

#[macro_export]
macro_rules! rc_ref_cell {
    ($a:expr) => {
        Rc::new(RefCell::new($a))
    };
}
