use rand::RngExt;
use std::vec;

#[derive(Debug)]
pub struct EncodedPayload {
    coeffs: Vec<u8>,
    payload: Vec<u8>,
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

pub fn split(payload: Vec<u8>, chunk_size: usize) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();

    for chunk in payload.chunks(chunk_size) {
        let mut p = chunk.to_vec();

        while p.len() < chunk_size {
            p.push(0);
        }

        packets.push(p);
    }
    packets
}

pub fn generate_encoded_payload(packets: &Vec<Vec<u8>>, count: u8) -> Vec<EncodedPayload> {
    let mut encoded = Vec::new();

    for _ in 0..count {
        encoded.push(encode(packets));
    }
    encoded
}

pub fn encode(packets: &Vec<Vec<u8>>) -> EncodedPayload {
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

    EncodedPayload { coeffs, payload }
}

fn main() {
    let payload = b"123rlnc___rocks!!".to_vec();
    let packets = split(payload.clone(), 5);

    println!(
        "payload: {:?},\nbytes: {:?}",
        String::from_utf8(payload.clone()).unwrap(),
        payload
    );

    println!("\nPackets:");
    for p in &packets {
        println!("{:?}", p);
    }

    println!("\n");
    let encoded_payloads = generate_encoded_payload(&packets, 5);
    for enc in &encoded_payloads {
        println!("{:?}", enc);
    }

    // This will happen in the local end of the receiver
    let selected: Vec<_> = encoded_payloads.into_iter().take(packets.len()).collect();
    let (mut matrix, mut data) = build_linear_system(selected);

    println!("\nMatrix before elimination:");
    println!("{:?}", matrix);

    gaussian_elimination(&mut matrix, &mut data);

    println!("\nMatrix after elimination:");
    println!("{:?}", matrix);

    println!("\nDecoded packets:");
    for d in &data {
        println!("{:?}", d);
    }

    // reconstruct
    let output = reconstruct(data);

    println!("\nReconstructed: {:?}", String::from_utf8_lossy(&output));
}

pub fn build_linear_system(packets: Vec<EncodedPayload>) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut matrix = Vec::new();
    let mut data = Vec::new();

    for pkt in packets {
        matrix.push(pkt.coeffs.clone());
        data.push(pkt.payload.clone());
    }

    (matrix, data)
}

fn gaussian_elimination(matrix: &mut Vec<Vec<u8>>, data: &mut Vec<Vec<u8>>) {
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
        let inv = gf_inv(matrix[i][i]); // we’ll define this next

        for j in 0..n {
            matrix[i][j] = gf_mul(matrix[i][j], inv);
        }

        for j in 0..data[i].len() {
            data[i][j] = gf_mul(data[i][j], inv);
        }

        // eliminate below
        for k in 0..n {
            if k != i {
                let factor = matrix[k][i];

                for j in 0..n {
                    let val = gf_mul(factor, matrix[i][j]);
                    matrix[k][j] ^= val;
                }

                for j in 0..data[k].len() {
                    let val = gf_mul(factor, data[i][j]);
                    data[k][j] ^= val;
                }
            }
        }
    }
}

fn reconstruct(data: Vec<Vec<u8>>) -> Vec<u8> {
    let mut result = Vec::new();

    for packet in data {
        result.extend(packet);
    }

    // result.truncate(original_len); // remove padding
    result
}
