pub const MS_PER_DAY: u64 = 86_400_000; // 24 h * 3 600 s * 1 000 ms
pub const MS_PER_HOUR: u64 = 3_600_000; // 3 600 s * 1 000 ms
pub const MS_PER_MINUTE: u64 = 60_000; // 60 s * 1 000 ms
pub const MS_PER_SECOND: u64 = 1_000; // 1 s * 1 000 ms

/// Shortcut for epoch ms.
pub fn emit_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("failed to get timestamp")
        .as_secs()
}

pub fn emit_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("failed to get timestamp")
        .as_millis()
}

pub fn emit_micros() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("failed to get timestamp")
        .as_micros()
}

pub fn emit_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("failed to get timestamp")
        .as_nanos()
}
/// Given any epoch‐ms, return (start_of_utc_day, start_of_next_utc_day).
/// All in UTC.  If you need local‐midnight instead, just add/subtract your fixed offset.
pub fn day_bounds_from_epoch_ms(ms: u64) -> (u64, u64) {
    let day_idx = ms / MS_PER_DAY; // how many whole days since epoch
    let day_start = day_idx * MS_PER_DAY; // ms at 00:00 UTC that day
    let next_start = day_start + MS_PER_DAY;
    (day_start, next_start)
}

/// Break an epoch‐ms into (hour, minute, second, millisecond) within its UTC day.
pub fn time_hms_from_epoch_ms(ms: u64) -> (u64, u64, u64, u64) {
    let ms_into_day = ms % MS_PER_DAY;
    let hour = ms_into_day / MS_PER_HOUR;
    let min = (ms_into_day % MS_PER_HOUR) / MS_PER_MINUTE;
    let sec = (ms_into_day % MS_PER_MINUTE) / MS_PER_SECOND;
    let msec = ms_into_day % MS_PER_SECOND;
    (hour, min, sec, msec)
}

pub fn make_scan_key(ts: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&ts.to_be_bytes()); // timestamp prefix
    // the rest you can zero out or set to 0 so it's the very first key at that ts
    key[8] = 0; // minimum kind
    // seq bytes already zero
    key
}
