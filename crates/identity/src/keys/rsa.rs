use anyhow::Result;

use base58::ToBase58;
use rand::thread_rng;
use rsa::pkcs1v15::Signature;
use rsa::pkcs8::EncodePrivateKey;
use rsa::signature::{Keypair, SignerMut, Verifier};
use rsa::{
    pkcs1::{EncodeRsaPrivateKey, EncodeRsaPublicKey},
    pkcs1v15::{SigningKey, VerifyingKey},
    pkcs8::DecodePrivateKey,
    RsaPrivateKey, RsaPublicKey,
};
use sha2::{Digest, Sha256};

use crate::traits::core::IKeys;

#[derive(Debug, Clone)]
pub struct RsaKeyPair {
    pub private_key: RsaPrivateKey,
    pub public_key: RsaPublicKey,
    signer: Signer,
}

#[derive(Clone, Debug)]
struct Signer {
    signing_key: SigningKey<Sha256>,
    verifying_key: VerifyingKey<Sha256>,
}

impl RsaKeyPair {
    pub fn generate() -> Result<Self> {
        let mut rng = thread_rng();
        let private_key =
            RsaPrivateKey::new(&mut rng, 2048).expect("Failed to crete RSA private key");

        let public_key = RsaPublicKey::from(&private_key);
        let signing_key = SigningKey::<Sha256>::new(private_key.clone());

        Ok(Self {
            private_key,
            public_key,
            signer: Signer {
                signing_key: signing_key.clone(),
                verifying_key: signing_key.verifying_key(),
            },
        })
    }

    pub fn from_pem(pem: &str) -> Result<Self> {
        let private_key = RsaPrivateKey::from_pkcs8_pem(pem)?;
        let public_key = RsaPublicKey::from(&private_key);
        let signing_key = SigningKey::new(private_key.clone());

        Ok(Self {
            private_key,
            public_key,
            signer: Signer {
                signing_key: signing_key.clone(),
                verifying_key: signing_key.verifying_key(),
            },
        })
    }

    pub fn to_pem(&self) -> Result<String> {
        Ok(self
            .private_key
            .to_pkcs1_pem(Default::default())?
            .to_string())
    }

    pub fn to_hex(&self) -> Result<String> {
        let der = self.private_key.to_pkcs8_der().unwrap();
        Ok(hex::encode(der.as_bytes()))
    }

    pub fn from_hex(hex: String) -> Result<Self> {
        let bytes = hex::decode(hex).unwrap();
        let pvt_key = RsaPrivateKey::from_pkcs8_der(&bytes).unwrap();
        let pub_key = RsaPublicKey::from(&pvt_key);
        let signing_key = SigningKey::<Sha256>::new(pvt_key.clone());

        Ok(Self {
            private_key: pvt_key,
            public_key: pub_key,
            signer: Signer {
                signing_key: signing_key.clone(),
                verifying_key: signing_key.verifying_key(),
            },
        })
    }

    pub fn peer_id(&self) -> String {
        let der_bytes = self
            .public_key
            .to_pkcs1_der()
            .expect("Failed to encode public key")
            .as_bytes()
            .to_vec();

        let mut hasher = Sha256::new();
        hasher.update(der_bytes);
        let digest = hasher.finalize();

        let short_hash = &digest[..32];
        short_hash.to_base58()
    }
}

impl IKeys<Signature> for RsaKeyPair {
    fn public_key(&self) -> String {
        self.public_key
            .to_pkcs1_pem(Default::default())
            .unwrap()
            .to_string()
    }

    fn sign(&self, msg: &[u8]) -> Result<Signature> {
        Ok(self.signer.signing_key.clone().sign(msg))
    }

    fn verify(&self, msg: &[u8], sig: &Signature) -> bool {
        let result = self.signer.verifying_key.verify(msg, sig);
        if result.is_ok() {
            return true;
        }
        false
    }
}
