use std::env;
pub fn setup_log() {
    if env::var_os("RUST_LOG").is_none() {
        // Set `RUST_LOG=debug` to see debug logs,
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();
}
