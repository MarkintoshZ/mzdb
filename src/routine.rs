use crate::args::Args;
use crate::chord::Chord;
use crate::mzdb::{LookupRequest, WhothisRequest};
use crate::node_info::NodeInfo;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn start(self_info: NodeInfo, chord: Arc<Mutex<Chord>>, args: Args) {
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    // connect to successor
    info!("Connecting to successor");
    let mut successor = Chord::create_conn(&args.successor.to_string())
        .await
        .expect("failed to connect to successor");
    let successor_node = successor.whothis(WhothisRequest {}).await.unwrap();
    chord
        .lock()
        .await
        .set_finger(0, successor_node.into_inner().into(), successor);
    info!("Successor connected");
    debug!("{:?}", &chord.lock().await.fingers);

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // build finger table
    info!("Building finger table");
    let finger_idx = (0..args.m)
        .map(|i| 2u64.pow(i as u32))
        .map(|i| (i + self_info.key_slot) % 2u64.pow(args.m as u32));

    for (i, curr_key_slot) in finger_idx.enumerate().skip(1) {
        let mut chord_guard = chord.lock().await;
        if let Some(suc) = chord_guard.lookup(curr_key_slot) {
            let prev_key = curr_key_slot / 2;
            if (prev_key < suc.key_slot && curr_key_slot <= suc.key_slot)
                || (prev_key > suc.key_slot
                    && (curr_key_slot <= suc.key_slot || curr_key_slot > prev_key))
            {
                chord_guard.fingers[i] = Some(suc);
            } else {
                let mut successor = chord_guard.lookup_conn(curr_key_slot).unwrap();
                let successor_node = successor
                    .lookup(LookupRequest {
                        key_slot: curr_key_slot,
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
    info!("Finger table built");
    chord
        .lock()
        .await
        .fingers
        .iter()
        .enumerate()
        .for_each(|(i, f)| {
            debug!("{}: {:?}", i, f);
        });
}
