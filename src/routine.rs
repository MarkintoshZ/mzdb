use crate::args::Args;
use crate::chord::Chord;
use crate::mzdb::{LookupRequest, WhothisRequest};
use crate::node_info::NodeInfo;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn start(self_info: NodeInfo, chord: Arc<Mutex<Chord>>, args: Args) {
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    // connect to successor
    let mut successor = Chord::create_conn(&args.successor.to_string())
        .await
        .expect("failed to connect to successor");
    let successor_node = successor.whothis(WhothisRequest {}).await.unwrap();
    chord
        .lock()
        .await
        .set_finger(0, successor_node.into_inner().into(), successor);
    dbg!(&chord.lock().await.fingers);

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // build finger table
    let finger_idx = (0..args.m)
        .map(|i| 2u64.pow(i as u32))
        .map(|i| (i + self_info.key) % 2u64.pow(args.m as u32));

    for (i, curr_key) in finger_idx.enumerate().skip(1) {
        let mut chord_guard = chord.lock().await;
        if let Some(suc) = chord_guard.lookup(curr_key) {
            let prev_key = curr_key / 2;
            if (prev_key < suc.key && curr_key <= suc.key)
                || (prev_key > suc.key && (curr_key <= suc.key || curr_key > prev_key))
            {
                chord_guard.fingers[i] = Some(suc);
            } else {
                let mut successor = chord_guard.lookup_conn(curr_key).unwrap();
                let successor_node = successor
                    .lookup(LookupRequest {
                        key: curr_key,
                        relay: true,
                    })
                    .await
                    .unwrap()
                    .into_inner();
                let successor_conn = Chord::create_conn(&successor_node.addr).await.unwrap();
                chord_guard.set_finger(i, successor_node.into(), successor_conn);
            }
        }
        drop(chord_guard);
    }
    chord
        .lock()
        .await
        .fingers
        .iter()
        .enumerate()
        .for_each(|(i, f)| {
            println!("{}: {:?}", i, f);
        });
}
