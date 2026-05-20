suppose a payload is 3 packets

```
P1 = [a a a a]
P2 = [b b b b]
P3 = [c c c c]
```

instra of sending them directly, we send random mixtures:
```
E1 = 2·P1 + 5·P2 + 7·P3
E2 = 1·P1 + 0·P2 + 3·P3
E3 = 4·P1 + 9·P2 + 2·P3
E4 = 7·P1 + 4·P2 + 1·P3
```

Each encoded packet carries: 
    - the coefficients -> [2, 5, 7]
    - the resulting mixed payload eg. E1, E2, E3

Receiver collects enough of these and solves a linear system to recover P1, P2, P3. That's RLNC

## pipeline (step-by-step)

### 1. Input data
```rust
let input = b"1234hello_world__is_rlnc".to_vec()
```

Think of this as a long byte stream:
```
[49, 50, 51, 52, 104, 101, ...]
```

### 2. Splitting into packets
```rust
let packets = split(input.clone(), 5);
```

here we divide the data into fixed-size chunks:
```
[49,50,51,52,104]      → Packet 1
[101,108,108,111,95]   → Packet 2
[119,111,114,108,100]  → Packet 3
...
```
if last chunk is short -> padded with 0

```
Original stream
---------------------------
|  P1  |  P2  |  P3  | P4 |
---------------------------
```

### 3. Encoding (core RLNC idea)

```rust
let encoded = generate(&packets, generation_size + 2);
```

Each encoded packet is:
```
E = c1·P1 + c2·P2 + ... + cn·Pn
```
where: 
    - `ci` are random u8 values
    - operation happen in `GF(256)` (finite field)

Visual
```
P1: [a1 a2 a3 a4 a5]
P2: [b1 b2 b3 b4 b5]
P3: [c1 c2 c3 c4 c5]

E1 = [x x x x x]  ← mix of all packets
E2 = [y y y y y]
E3 = [z z z z z]
E4 = [w w w w w]
```

Here each E also carries:
```
coeffs = [c1,c2,c3]
```

### 4. Transmission assumption

We generate more packets than needed: `generation_size + 2`

Why? packets may be lost, and in our example for `generation_size = 3`, we need alteast 3 encoded packets out of 4 `E1, E2, E3, E4`, to reconstruct the original packets `P1, P2, P3`

### 5. Building the system

```rust
(matrix, data) = build_system(&selected);
```
Now we construct packets into:

Matrix (coefficients)
```
[ c11 c12 c13 ]
[ c21 c22 c23 ]
[ c31 c32 c33 ]
```

Data (encoded payloads)
```
[ E1 ]
[ E2 ]
[ E3 ]
```

This represents: `C × P = E`
Where:
    - `C` = coefficient matrix (known to the receiver)
    - `P` = original packets (unknown to the receiver, and have to find it)
    - `E` = encoded packets (known to the receiver)

### 6. Gaussian elimination (decoding)

```rust
gaussian_elimination(&mut matrix, &mut data);
```

here we are solving: `C x P = E` to get `P = C⁻¹ × E`

### 7. Reconstruction
```rust
let output = reconstruct(data, original_len);
```

## Big picture flow
```
[ Original Data ]
        ↓
[ Split into packets ]
        ↓
[ Random linear combinations ]
        ↓
[ Encoded packets transmitted ]
        ↓
[ Receiver collects enough packets ]
        ↓
[ Solve linear system ]
        ↓
[ Recover original packets ]
        ↓
[ Reconstruct data ]
```

## Pros of RLNC
- Extremely robust to packet loss: don't need specific packets -- just enough independent ones. Perfect for unreliable networks

- no retransmission needed: unlike TCP, we just keep sending random combinations
- great for p2p / multicast: Multiple node can mix packets further, forward new combinations

- This will work well with mesh networks and gossip protocols

## Cons of RLNC
- extra overhead in bandwidth for more number of encoded packets corresponding to less original packets

# Expected Output of the code in main.rs
```
soi@soiarch ~/Desktop/rnet ❯ cargo run --bin rlnc               
   Compiling example-rlnc v0.1.0 (/home/soi/Desktop/rnet/examples/rlnc)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.49s
     Running `target/debug/rlnc`
Original: "1234hello_world__is_rlnc",
Bytes: [49, 50, 51, 52, 104, 101, 108, 108, 111, 95, 119, 111, 114, 108, 100, 95, 95, 105, 115, 95, 114, 108, 110, 99]
Packets:
[49, 50, 51, 52, 104]
[101, 108, 108, 111, 95]
[119, 111, 114, 108, 100]
[95, 95, 105, 115, 95]
[114, 108, 110, 99, 0]

Encoded packets:
coeffs: [207, 241, 34, 130, 216], payload: [0, 60, 221, 203, 4]
coeffs: [249, 51, 26, 181, 83], payload: [143, 92, 125, 103, 237]
coeffs: [107, 170, 20, 18, 39], payload: [141, 241, 74, 49, 166]
coeffs: [245, 118, 200, 170, 0], payload: [53, 244, 233, 0, 140]
coeffs: [72, 94, 184, 8, 194], payload: [192, 38, 246, 245, 175]
coeffs: [86, 74, 187, 79, 128], payload: [3, 166, 102, 202, 147]
coeffs: [199, 132, 45, 60, 148], payload: [164, 177, 237, 50, 18]

Matrix before elimination:
[[207, 241, 34, 130, 216], [249, 51, 26, 181, 83], [107, 170, 20, 18, 39], [245, 118, 200, 170, 0], [72, 94, 184, 8, 194]]

Matrix after elimination:
[[1, 0, 0, 0, 0], [0, 1, 0, 0, 0], [0, 0, 1, 0, 0], [0, 0, 0, 1, 0], [0, 0, 0, 0, 1]]

Decoded packets:
[49, 50, 51, 52, 104]
[101, 108, 108, 111, 95]
[119, 111, 114, 108, 100]
[95, 95, 105, 115, 95]
[114, 108, 110, 99, 0]

Reconstructed: "1234hello_world__is_rlnc"

✅ SUCCESS: Data reconstructed correctly
```