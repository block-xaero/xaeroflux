pub fn emit_path_with_prefix(configured_super_dir: &str, event_processor_name: &str) -> String {
    format!("{}/{}", configured_super_dir, event_processor_name)
}
