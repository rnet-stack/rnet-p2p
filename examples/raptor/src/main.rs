use rand::seq::SliceRandom;
use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};

// --- AFG Protocol Hardcoded Constants ---
// These are known by every node in the mesh natively.
const SYMBOL_SIZE: u16 = 1280;
const SOURCE_BLOCKS: u8 = 1;
const SUB_BLOCKS: u16 = 1;
const ALIGNMENT: u8 = 1;

fn main() {
    // 1. Generate some dummy data (e.g., a serialized Protobuf message or block)
    let payload_size = 50_000;
    let original_data: Vec<u8> = (0..payload_size).map(|i| (i % 256) as u8).collect();

    // 2. Configure the Encoder (Sender Side)
    // We use our hardcoded SYMBOL_SIZE. 
    let encoder = Encoder::with_defaults(&original_data, SYMBOL_SIZE);

    // Notice we DO NOT use `encoder.get_config()` here anymore.
    // In your actual AFG protocol, the sender will pack `payload_size` (50_000) 
    // into the 4-byte custom UDP header of every packet it transmits.

    // 3. Generate packets
    let repair_packets = 20;
    let mut encoded_packets: Vec<EncodingPacket> = encoder.get_encoded_packets(repair_packets);

    let total_generated = encoded_packets.len();

    // 4. Simulate a lossy UDP network
    let mut rng = rand::rng();
    encoded_packets.shuffle(&mut rng);

    // Drop 15% of the packets to simulate network packet loss
    let loss_rate = 0.15;
    let packets_to_keep = ((1.0 - loss_rate) * total_generated as f64) as usize;
    encoded_packets.truncate(packets_to_keep);

    println!("Total packets generated: {}", total_generated);
    println!("Packets successfully delivered: {}", encoded_packets.len());

    // 5. Decode on the receiver side
    // SIMULATION: The receiver extracts the `transfer_length` from the first UDP packet header it sees.
    let received_transfer_length = payload_size as u64;

    // Manually construct the OTI using the globally known constants + the received length
    let oti = ObjectTransmissionInformation::new(
        received_transfer_length,
        SYMBOL_SIZE,
        SOURCE_BLOCKS,
        SUB_BLOCKS,
        ALIGNMENT,
    );

    // Initialize the decoder using the manually constructed OTI
    let mut decoder = Decoder::new(oti);
    let mut decoded_payload: Option<Vec<u8>> = None;
    let mut packets_used = 0;

    for packet in encoded_packets {
        packets_used += 1;
        
        // Feed packets into the decoder one by one.
        if let Some(result) = decoder.decode(packet) {
            decoded_payload = Some(result);
            break;
        }
    }

    // 6. Verify the outcome
    match decoded_payload {
        Some(recovered_data) => {
            assert_eq!(original_data, recovered_data);
            println!(
                "Success! Reconstructed the payload using {} packets.",
                packets_used
            );
        }
        None => {
            println!("Failed to reconstruct: Not enough packets received.");
        }
    }
}