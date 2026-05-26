- drop 25% of all packets on loopback: `sudo tc qdisc add dev lo root netem loss 25%`
- restore the network config: `sudo tc qdisc del dev lo root`

- `tokio::time::sleep(Duration<_>)`, only suspends the async task/future that hits this line, and `std::thread::sleep(Duration<_>)`, suspends the whole main thread.

- In `Udp`, if a 3000 bytes packets is in arrival, and split into fragments. then all fragments must arrive, if even one fragment is lost, then the entire packet is lost.

- `rtt` behaviour in ping experiments:
  - Sequential sequence -> 1s delay between pings
  - burst sequence -> rapid back-to-back pings
  - under multi-threaded tokio runtime + single-threaded `(flavor="current_thread")` runtime
    - Sequential: rtt `~180-320μs` + high jitter + no convergence
    - burst (no-delay) rtt drops over time + converges to `~30μs`
    - single-threaded runtime: lower jitter + converges to `~28μs`
  - RTT = network + syscalls + runtime scheduling + cache effects
  - **why sequential slower ?**
    - `sleep()` causes: task descheduling + wake-up delay + possible thread migration
    - CPU caches go cold + kernel buffers drain -> each ping starts from a cold system state
  - **why burst faster ?**
    - no sleeping -> no scheduler overhead
    - same thread + same task + hot CPU caches + active kernel buffers
    - system stays warm -> latency drops and stabalizes 
  - Effect of `current-thread` -> removes cross-thread scheduling + no work steals + better cache locality.
  - The difference is mainly due to runtime scheduling + cache locality + system warm-up

- `dyn` compatibility: means if we can turn a trait into trait object
  - Things that break `dyn` compatibility:
    - `fn foo<T>(&self, x: T)` :- infinite possibilties
    - `fn clone(&self) -> Self` :- returning self, can't be known at compile time.
    - `fn compare(&self, other: Self)` :- using self in arguments
    - `fn consume(self)` :- methods taking self by value
  - Basically try to keep no generics on methods
  - See `INode` and `IMuxedConn`

- Trait objects are written as `&dyn INode`

- `Send` is only required when an async future is created and promised to be movable across threads, the promise is generally made by `#[async_trait]`, but we can also opt-out of it. In Rnet infra, I doubt I moved the TcpStreams to multiple threads.

- Generics must return concrete types at compile time. See distinction in `MplexConn<T>` and `MuxedConn` struct, as muxed_conn struct can return either mplex_conn or yamux_conn based on runtime inputs, which is not allowed in Rust, so can't use generics here.

- Everytime we use `#[async_trait]`, the async methods return `Send` futures by default. If everything inside the async method is already `Send` -> we won't notice. But if anything captured is not provably `Send` -> the compiler will force

- use trait-generics for non-associated types, and for associated type, declare them inside the trait declaration.

- non-generic impl/traits, we have to use concrete type of structs, whose types are defined at runtime. But with generic impl/traits, we can use structs with generic type declaration, like `Foo<f32>` and `Foo<char>` can both use `Value<T>` trait.

- Generic & trait bounds Vs trait objects: Only one object-type allowed when using generic trait bounds, but can use multiple object-type satisfying the trait in trait objects. Generic trait bounds are more performant.

- Stack: fixed-size data known at compile time, ownership is clear and local. Heap, a big pool of memory managed at runtime, allocation requires taking to an allocator, lifetime is not tied to a scope by default. In heap, there is runtime overhead, only during allocation.

- Needed the traits: `Hash, PartialEq, Eq` for inserting custom key-type in HashMaps

- Interior mutability over &mut self: In the Floosub API methods like `handle_dead_peers(&self, ...)` we can take a mut reference to something like `self.floodsub_store` if it is guarded inside shared state `Arc<Mutex<_>>`. This is possible via runtime-enforced interior mutability rather than exclusive compile-time borrowing.

- In floodsub handle*api impl, moved around the Arc<Mutex<*>> of FloodsubPeers to conserve the &self consumption of the function so we can use the floodsub instance even after starting the handle_api tokio::spawned-task, in the application code. In contrast to this, we cannot use th host-instance after starting the BasicHost::run().

- For continuous read/write operability on TcpStreams, used the tokio::select! pattern to do execute whatever Future gets complete and rerun the loop.
