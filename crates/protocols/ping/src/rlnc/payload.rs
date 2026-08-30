use anyhow::Result;
use rand::RngExt;
use serde::{Deserialize, Serialize};

pub const DEFAULT_CHUNK_SIZE: u8 = 5;
pub const DEFAULT_ENCODE_SET_LEN: u8 = 10;

#[derive(Debug, Serialize, Deserialize)]
pub struct EncodedPayload {
    pub coeffs: Vec<u8>,
    payload: Vec<u8>,
    pub id: String,
}

impl EncodedPayload {
    pub fn split(payload: &[u8]) -> Vec<Vec<u8>> {
        let mut packets = Vec::new();

        for chunk in payload.chunks(DEFAULT_CHUNK_SIZE as usize) {
            let mut p = chunk.to_vec();

            while p.len() < DEFAULT_CHUNK_SIZE as usize {
                p.push(0);
            }

            packets.push(p);
        }
        packets
    }

    pub fn generate_encoded_set(packets: &[Vec<u8>], id: String) -> Vec<EncodedPayload> {
        let mut encoded = Vec::new();

        for _ in 0..DEFAULT_ENCODE_SET_LEN {
            encoded.push(EncodedPayload::encode(packets, id.clone()));
        }
        encoded
    }

    fn encode(packets: &[Vec<u8>], id: String) -> EncodedPayload {
        let mut rng = rand::rng();

        let n = packets.len();
        let size = packets[0].len();

        let mut coeffs = vec![0u8; n];
        let mut payload = vec![0u8; size];

        for i in 0..n {
            let coeff = rng.random::<u8>();
            coeffs[i] = coeff;

            for j in 0..size {
                let val = gf_mul(coeff, packets[i][j]);
                payload[j] = gf_add(payload[j], val);
            }
        }

        EncodedPayload {
            coeffs,
            payload,
            id,
        }
    }

    pub fn build_linear_system(encoded_vec: &Vec<EncodedPayload>) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let mut matrix = Vec::new();
        let mut data = Vec::new();

        for pkt in encoded_vec {
            matrix.push(pkt.coeffs.clone());
            data.push(pkt.payload.clone());
        }

        (matrix, data)
    }

    pub fn gaussian_elimination(matrix: &mut [Vec<u8>], data: &mut [Vec<u8>]) {
        let n = matrix.len();

        for i in 0..n {
            // find pivot
            let mut pivot = i;
            while pivot < n && matrix[pivot][i] == 0 {
                pivot += 1;
            }

            if pivot == n {
                continue; // singular
            }

            matrix.swap(i, pivot);
            data.swap(i, pivot);

            // normalize pivot row
            let inv = gf_inv(matrix[i][i]); // w'll define this next

            for val in matrix[i].iter_mut().take(n) {
                *val = gf_mul(*val, inv);
            }

            for j in 0..data[i].len() {
                data[i][j] = gf_mul(data[i][j], inv);
            }

            // eliminate below
            let pivot_row = matrix[i].clone();

            for k in 0..n {
                if k != i {
                    let factor = matrix[k][i];

                    for (val, &pivot) in matrix[k].iter_mut().zip(pivot_row.iter()).take(n) {
                        *val ^= gf_mul(factor, pivot);
                    }

                    for j in 0..data[k].len() {
                        let val = gf_mul(factor, data[i][j]);
                        data[k][j] ^= val;
                    }
                }
            }
        }
    }

    pub fn reconstruct(data: Vec<Vec<u8>>) -> Vec<u8> {
        let mut result = Vec::new();

        for packet in data {
            result.extend(packet);
        }

        // result.truncate(original_len); // remove padding
        result
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }

    pub fn from_bytes(frame: Vec<u8>) -> Result<EncodedPayload> {
        Ok(bincode::deserialize(&frame).unwrap())
    }
}

fn gf_add(a: u8, b: u8) -> u8 {
    a ^ b
}

fn gf_mul(mut a: u8, mut b: u8) -> u8 {
    let mut p = 0;

    for _ in 0..8 {
        if b & 1 != 0 {
            p ^= a;
        }

        let carry = a & 0x80;
        a <<= 1;

        if carry != 0 {
            a ^= 0x1b;
        }

        b >>= 1;
    }

    p
}

fn gf_inv(x: u8) -> u8 {
    for i in 1..=255 {
        if gf_mul(x, i) == 1 {
            return i;
        }
    }

    panic!("No inverse");
}
