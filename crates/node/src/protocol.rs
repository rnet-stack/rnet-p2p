use floodsub::pubsub::FloodSub;
use ping::handler::Ping;

pub enum InnerProtocol {
    Floodsub(FloodSub),
    Ping(Ping),
}

pub enum InnerProtocolOpt {
    Floodsub,
    Ping { enable_rlnc: bool },
}
