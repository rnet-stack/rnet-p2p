use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Error, Result};
use identity::traits::muxer::IMuxedStream;
use tokio::{sync::Mutex, time::timeout};
use tracing::error;

use crate::{handler::PING_LENGTH, rlnc::payload::EncodedPayload};

pub struct PingRLNC {
    cache: Arc<Mutex<HashMap<String, Vec<EncodedPayload>>>>,
    seen: Arc<Mutex<HashSet<String>>>,
}

impl PingRLNC {
    pub fn new() -> Arc<PingRLNC> {
        Arc::new(PingRLNC {
            cache: Arc::new(Mutex::new(HashMap::new())),
            seen: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    pub async fn handle_incoming(&self, payload: EncodedPayload) -> Option<Vec<u8>> {
        let mut seen = self.seen.lock().await;
        let mut cache = self.cache.lock().await;

        if seen.contains(&payload.id) {
            return None;
        }

        match cache.get_mut(&payload.id) {
            None => {
                cache.insert(payload.id.clone(), vec![payload]);
            }
            Some(vec) => match vec.len() == (payload.coeffs.len() - 1) {
                true => {
                    // warn!("RLNC handle_incoming: reconstruction possible");

                    let (_, encoded_vec) = cache.remove_entry(&payload.id).unwrap();
                    let reconstructed = self.reconstruct(&encoded_vec).unwrap();

                    seen.insert(payload.id);
                    return Some(reconstructed);
                }
                false => {
                    // warn!("RLNC handle_incoming: part received");
                    vec.push(payload);
                }
            },
        }

        None
    }

    pub async fn ping(
        &self,
        stream: &mut Box<dyn IMuxedStream + Send + Sync + 'static>,
        id: String,
    ) -> Result<u128> {
        let payload = vec![0x01; PING_LENGTH];
        let timeout_duration = Duration::from_secs(2);

        let rlnc_frames = self.formulate(&payload, &id).unwrap();

        let rtt = match stream.is_initiator() {
            true => {
                let start = Instant::now();

                let exchange = async {
                    for frame in rlnc_frames {
                        stream.write(&frame.as_bytes()).await?;
                    }

                    loop {
                        let frame = stream.read().await.unwrap();
                        let encoded_payload = EncodedPayload::from_bytes(frame).unwrap();

                        match self.handle_incoming(encoded_payload).await {
                            Some(_reconstructed) => {
                                // assert_eq!(reconstructed, payload);

                                break;
                            }
                            None => continue,
                        }
                    }

                    Ok(())
                };

                match timeout(timeout_duration, exchange).await {
                    Ok(Ok(())) => start.elapsed().as_micros(),

                    Ok(Err(e)) => {
                        error!("Ping exchange failed: {}", e);
                        return Err(e);
                    }

                    Err(_) => {
                        return Err(Error::msg(format!("ping timeout: {:?}", timeout_duration)));
                    }
                }
            }
            false => {
                let exchange = async {
                    loop {
                        let frame = stream.read().await.unwrap();
                        let encoded_payload = EncodedPayload::from_bytes(frame).unwrap();

                        match self.handle_incoming(encoded_payload).await {
                            Some(reconstructed) => {
                                // assert_eq!(reconstructed, payload);

                                let rlnc_frames = self.formulate(&reconstructed, &id).unwrap();
                                for frame in rlnc_frames {
                                    stream.write(&frame.as_bytes()).await.unwrap();
                                }

                                break;
                            }
                            None => continue,
                        }
                    }

                    Ok(())
                };

                match timeout(timeout_duration, exchange).await {
                    Ok(Ok(())) => 0,

                    Ok(Err(e)) => {
                        error!("Ping exchange failed: {}", e);
                        return Err(e);
                    }

                    Err(_) => {
                        return Err(Error::msg("ping timeout"));
                    }
                }
            }
        };

        Ok(rtt)
    }

    fn formulate(&self, payload: &[u8], id: &str) -> Result<Vec<EncodedPayload>> {
        let packets = EncodedPayload::split(payload);
        let encoded_set = EncodedPayload::generate_encoded_set(&packets, id.to_owned());

        Ok(encoded_set)
    }

    fn reconstruct(&self, encoded_vec: &Vec<EncodedPayload>) -> Result<Vec<u8>> {
        let (mut matrix, mut data) = EncodedPayload::build_linear_system(encoded_vec);
        EncodedPayload::gaussian_elimination(&mut matrix, &mut data);
        let reconstructed = EncodedPayload::reconstruct(data);

        Ok(reconstructed)
    }
}
