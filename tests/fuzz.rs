extern crate loom;

use loom::thread;

#[path = "../src/channel.rs"]
mod spmc;

#[test]
fn smoke() {

    loom::model(|| {
        let (tx, rx) = spmc::channel::<String>();

        let th = thread::spawn(move || {
            while let Ok(_s) = rx.recv() {
                // ok
            }
        });

        tx.send("hello".into()).unwrap();
        drop(tx);
        th.join().unwrap();
    });
}

#[test]
fn no_send() {
    loom::model(|| {
        let (tx, rx) = spmc::channel::<String>();

        let th = thread::spawn(move || {
            while let Ok(_s) = rx.recv() {
                unreachable!("no sends");
            }
        });

        drop(tx);
        th.join().unwrap();
    });
}

#[test]
fn multiple_threads() {
    loom::model(|| {
        let (tx, rx) = spmc::channel::<String>();


        let mut threads = Vec::new();

        threads.push(thread::spawn(move || {
            tx.send("hello".into()).unwrap();
            tx.send("world".into()).unwrap();
        }));

        for _ in 0..2 {
            let rx = rx.clone();
            threads.push(thread::spawn(move || {
                let mut cnt = 0;
                while let Ok(_s) = rx.recv() {
                    cnt += 1;
                }
                drop(cnt);
            }));
        }

        for th in threads {
            th.join().unwrap();
        }
    });
}

