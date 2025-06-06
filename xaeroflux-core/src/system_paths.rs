pub fn emit_control_path_with_subject_hash(
    configured_super_dir: &str,
    subject_hash: [u8; 32],
    event_processor_name: &str,
) -> String {
    let encoded_dir = hex::encode(subject_hash);
    let dir_prefix = encoded_dir.as_str();
    format!(
        "{}/{}/{}/control",
        configured_super_dir, dir_prefix, event_processor_name
    )
}

pub fn emit_data_path_with_subject_hash(
    configured_super_dir: &str,
    subject_hash: [u8; 32],
    event_processor_name: &str,
) -> String {
    let encoded_dir = hex::encode(subject_hash);
    let dir_prefix = encoded_dir.as_str();
    format!(
        "{}/{}/{}/data",
        configured_super_dir, dir_prefix, event_processor_name
    )
}
