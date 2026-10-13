//! BLOBs sent as `Chunk`s (§3.7.3.2, p.74-75): cutting them on the way out,
//! and the messages that carry them on the way in.
//!
//! Three protocols define a `Chunk` - Store (4), StoreNotification (5) and
//! StoreQuery (14) - as three schemas of the same shape, and it "works the
//! same in all cases". One trait covers them.

use retp_types::datatypes::Uuid;
use retp_types::datatypes::object::DataObject;
use retp_types::dispatch::Message;
use retp_types::protocol::{store, store_notification, store_query};

/// A `Chunk` of one of the three protocols that define one.
pub trait ChunkMessage: Sized {
    /// A piece of the BLOB `blob_id`; `last` on its final piece.
    fn piece(blob_id: Uuid, data: Vec<u8>, last: bool) -> Self;
}

macro_rules! chunk {
    ($ty:ty) => {
        impl ChunkMessage for $ty {
            fn piece(blob_id: Uuid, data: Vec<u8>, last: bool) -> Self {
                Self {
                    blob_id,
                    data,
                    r#final: last,
                }
            }
        }
    };
}
chunk!(store::Chunk);
chunk!(store_notification::Chunk);
chunk!(store_query::Chunk);

/// Move the data of every object larger than `piece` bytes into `Chunk`s.
///
/// Each such object gets a BLOB id from `new_id` - a UUID unique in the
/// session, which only the caller can promise - and an **empty** `data`
/// field; its bytes come back as chunks of at most `piece` bytes, the last
/// one `final` (§3.7.3.2 §1-2). The chunks of one object are all emitted
/// before the next object's: they **never interleave** (§3.7.3.2 §2.d).
///
/// The chunks go out after the message that carries the objects, in the same
/// multipart action, the last of them with the FIN (§3.7.3.2 §3).
pub fn chunk_objects<'a, C: ChunkMessage>(
    objects: impl IntoIterator<Item = &'a mut DataObject>,
    piece: usize,
    mut new_id: impl FnMut() -> Uuid,
) -> Vec<C> {
    assert!(piece > 0, "a chunk holds at least one byte");
    let mut chunks = Vec::new();
    for object in objects {
        if object.data.len() <= piece {
            continue;
        }
        let id = new_id();
        object.blob_id = Some(id);
        let data = std::mem::take(&mut object.data);
        let count = data.len().div_ceil(piece);
        for (index, bytes) in data.chunks(piece).enumerate() {
            chunks.push(C::piece(id, bytes.to_vec(), index + 1 == count));
        }
    }
    chunks
}

/// A received `Chunk`, whichever protocol it came in.
pub(super) fn as_chunk(message: &Message) -> Option<(&Uuid, &[u8], bool)> {
    match message {
        Message::StoreChunk(c) => Some((&c.blob_id, &c.data, c.r#final)),
        Message::StoreNotificationChunk(c) => Some((&c.blob_id, &c.data, c.r#final)),
        Message::StoreQueryChunk(c) => Some((&c.blob_id, &c.data, c.r#final)),
        _ => None,
    }
}

/// The data objects of a message that may announce BLOBs.
///
/// The four that carry `DataObject`s in the protocols that have a `Chunk`:
/// `GetDataObjectsResponse` and `PutDataObjects` (4), `ObjectChanged` (5),
/// `FindDataObjectsResponse` (14).
pub(super) fn objects_mut(message: &mut Message) -> Vec<&mut DataObject> {
    match message {
        Message::GetDataObjectsResponse(m) => m.data_objects.values_mut().collect(),
        Message::PutDataObjects(m) => m.data_objects.values_mut().collect(),
        Message::ObjectChanged(m) => vec![&mut m.change.data_object],
        Message::FindDataObjectsResponse(m) => m.data_objects.iter_mut().collect(),
        _ => Vec::new(),
    }
}
