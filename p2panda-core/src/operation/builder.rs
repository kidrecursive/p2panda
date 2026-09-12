// SPDX-License-Identifier: MIT OR Apache-2.0

use std::marker::PhantomData;

use cbor_core::Value;

use crate::hash::Hash;
use crate::identity::Signer;
use crate::logs::SeqNum;
use crate::operation::header::encode_header;
use crate::operation::{Header, MAX_HEADER_ITEM_LEN, PayloadSize};
use crate::traits::Extensions;
use crate::HeaderError;

/// Build & sign operations.
pub struct Builder<E> {
    payload_size: PayloadSize,
    payload_hash: Option<Hash>,
    seq_num: SeqNum,
    backlink: Option<Hash>,
    _marker: PhantomData<E>,
}

impl<E> Default for Builder<E>
where
    E: Extensions,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Builder<E>
where
    E: Extensions,
{
    pub fn new() -> Self {
        Self {
            payload_size: 0,
            payload_hash: None,
            seq_num: 0,
            backlink: None,
            _marker: PhantomData,
        }
    }

    /// Attach payload to operation.
    pub fn body(mut self, bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();

        self.payload_size = bytes.len() as PayloadSize;
        self.payload_hash = if self.payload_size == 0 {
            None
        } else {
            Some(Hash::digest(bytes))
        };

        self
    }

    /// Sets the "hash chain" values of this operation: sequence number and backlink.
    pub fn chain(mut self, seq_num: SeqNum, backlink: Hash) -> Self {
        self.seq_num = seq_num;

        if self.seq_num > 0 {
            self.backlink = Some(backlink);
        } else {
            // Ignore backlink if user tries to set one at seq_num = 0.
            self.backlink = None;
        }

        self
    }

    /// Number of operations this author has published to this log, begins with 0 and is always
    /// incremented by 1 with each new operation by the same author.
    pub fn seq_num(mut self, seq_num: SeqNum) -> Self {
        self.seq_num = seq_num;
        self
    }

    /// Hash of the previous operation of the same author and log. Can be omitted if first
    /// operation in log.
    pub fn backlink(mut self, backlink: Option<Hash>) -> Self {
        self.backlink = backlink;
        self
    }

    /// Encodes, signs and returns final header of operation.
    ///
    /// A custom header extensions type can be set here as well when required. It will be embedded
    /// in the header. Set this to `()` (unit-type) when extensions are not necessary.
    ///
    /// Returns [`HeaderError::TooLarge`] if the encoded header, or its `extensions` CBOR item,
    /// would exceed [`MAX_HEADER_ITEM_LEN`] -- the same bound `AnyHeader::decode` enforces on
    /// read (D3-o). We reject here, before signing and storing, rather than sign an operation we
    /// (or any peer) could never decode again.
    pub fn build<S: Signer>(self, signing_key: &S, extensions: E) -> Result<Header<E>, HeaderError> {
        let version = 1;

        let verifying_key = signing_key.verifying_key();

        let extensions_cbor = if !Header::<E>::has_zero_sized_extensions() {
            let extensions_cbor = Value::serialized(&extensions).expect("serializable extensions");
            Some(extensions_cbor)
        } else {
            None
        };

        if let Some(ref extensions_cbor) = extensions_cbor {
            let extensions_len = extensions_cbor.encode().len();
            if extensions_len > MAX_HEADER_ITEM_LEN {
                return Err(HeaderError::TooLarge {
                    len: extensions_len,
                    max: MAX_HEADER_ITEM_LEN,
                });
            }
        }

        let signing_bytes = encode_header(
            version,
            verifying_key,
            None,
            self.payload_size,
            self.payload_hash,
            self.seq_num,
            self.backlink,
            extensions_cbor.as_ref(),
        );

        let signature = signing_key.sign(&signing_bytes);

        let bytes = encode_header(
            version,
            verifying_key,
            Some(&signature),
            self.payload_size,
            self.payload_hash,
            self.seq_num,
            self.backlink,
            extensions_cbor.as_ref(),
        );

        if bytes.len() > MAX_HEADER_ITEM_LEN {
            return Err(HeaderError::TooLarge {
                len: bytes.len(),
                max: MAX_HEADER_ITEM_LEN,
            });
        }

        let digest = Hash::digest(&bytes);
        let size = bytes.len() as u32;

        Ok(Header {
            version,
            verifying_key,
            signature,
            payload_size: self.payload_size,
            payload_hash: self.payload_hash,
            seq_num: self.seq_num,
            backlink: self.backlink,
            extensions,
            extensions_cbor,
            digest,
            size,
        })
    }
}
