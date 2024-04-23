use log::debug;

pub fn log(string: String) {
    println!("[DEBUG]: {}", string);
}

#[macro_export]
macro_rules! rc_ref_cell {
    ($a:expr) => {
        Rc::new(RefCell::new($a))
    };
}
