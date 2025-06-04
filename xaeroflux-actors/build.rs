fn main() {
    let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    println!("cargo:rustc-env=PAGE_SIZE={}", ps);
}
