use raptorq::EncodingPacket;
use serde::{Deserialize, Serialize};

use crate::pubsub::{MessageID, TopicID};

#[derive(Debug, Serialize, Deserialize)]
pub struct PubsubMsg {
    // publish-msg -> sent via coded-msg using AFC, [ topic-id + coded-msg ]
    // sub-opts -> sent as it is, with UDP acknowledgement [ subscribe / unsubscribe ]
    // control-msg -> sent as it is, with UDP acknowledgement [ graft + prune + ihave + iwant ]
    // acks for control-msg + subopts 
    pub sub_opts: Option<SubOpts>,
    pub control: Option<ControlMsg>,
    pub publish: Vec<CodedMsg>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubOpts {
    pub subs: Vec<TopicID>,
    pub unsubs: Vec<TopicID>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ControlMsg {
    pub ihave: Vec<IHave>,
    pub iwant: Vec<IWant>,
    pub graft: Vec<TopicID>,
    pub prune: Vec<TopicID>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IHave {
    pub topic_id: TopicID,
    pub message_ids: Vec<MessageID>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IWant {
    pub message_ids: Vec<MessageID>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CodedMsg {
    // [ msg-id + codes ]
    pub topic_id: TopicID,
    pub msg_id: MessageID,
    pub transfer_length: u64,
    pub symbol: Vec<EncodingPacket>,
}
