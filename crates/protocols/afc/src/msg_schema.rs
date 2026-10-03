use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PubsubMsg {
    // publish-msg -> sent via coded-msg using AFC, [ topic-id + coded-msg ]
    // sub-opts -> sent as it is, with UDP acknowledgement [ subscribe / unsubscribe ]
    // control-msg -> sent as it is, with UDP acknowledgement [ graft + prune + ihave + iwant ]
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CodedMsg {
    // [ msg-id + codes ]
}
