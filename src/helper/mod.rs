use log::debug;

pub fn log(string: String) {
    debug!("[DEBUG]: {}", string);
}
