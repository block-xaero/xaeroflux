#[cfg(test)]
mod unsafe_run_tests {
    use xaeroflux::{Subject, XaeroEvent, core::initialize};

    #[test]
    fn test_subscribe_unsafe_run() {
        initialize();
        let subject = Subject::new("posts".into());
        let (tx, _rx) = crossbeam::channel::unbounded::<XaeroEvent>();
        let _xaeroflux_subscription = subject.subscribe(move |e| {
            tx.send(e).expect("failed to send event");
        });
        let _xf_handle = subject.unsafe_run();
    }
}
