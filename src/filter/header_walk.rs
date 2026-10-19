//! Header-area walk of one record: fixed header, origin, toplevel xid and
//! block locators, leaving block data, images and main data untouched.
//! Rejects every record `parse_record_from_bytes` rejects, so a caller can
//! fall back to the full parse for its error. Layout per PG
//! `src/include/access/xlogrecord.h`, unchanged from PG 15 through 19

use walrus::pg::walparser::{
    BKP_BLOCK_HAS_IMAGE, BLOCK_SIZE, RelFileNode, X_LOG_RECORD_HEADER_SIZE, XLR_BLOCK_ID_DATA_LONG,
    XLR_BLOCK_ID_DATA_SHORT, XLogRecord, XLogRecordBlockImageHeader, XLogRecordHeader,
};

const XLR_MAX_BLOCK_ID: u8 = 32;
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;
const BKPBLOCK_HAS_DATA: u8 = 0x20;
const BKPBLOCK_SAME_REL: u8 = 0x80;

/// Header-area reader; `remaining` counts body bytes not yet read or
/// reserved for the payloads that follow the header area
struct Cursor<'a> {
    buf: &'a [u8],
    remaining: usize,
}

impl Cursor<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.reserve(N)?;
        let (head, rest) = self.buf.split_first_chunk::<N>()?;
        self.buf = rest;
        Some(*head)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take::<1>().map(|[b]| b)
    }

    fn u16(&mut self) -> Option<u16> {
        self.take().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        self.take().map(u32::from_le_bytes)
    }

    fn reserve(&mut self, n: usize) -> Option<()> {
        self.remaining = self.remaining.checked_sub(n)?;
        Some(())
    }
}

/// `bytes` as a record holding only its header area, calling `block` per
/// block reference. `None` when `block` declines one or the record is
/// malformed
pub(crate) fn walk(
    bytes: &[u8],
    page_magic: u16,
    mut block: impl FnMut(RelFileNode) -> bool,
) -> Option<XLogRecord<'static>> {
    let (head, body) = bytes.split_first_chunk::<X_LOG_RECORD_HEADER_SIZE>()?;
    let u32_at = |at: usize| u32::from_le_bytes(head[at..at + 4].try_into().unwrap());
    let header = XLogRecordHeader {
        total_record_length: u32_at(0),
        xact_id: u32_at(4),
        prev_record_ptr: u64::from_le_bytes(head[8..16].try_into().unwrap()),
        info: head[16],
        resource_manager_id: head[17],
        crc32_hash: u32_at(20),
    };
    let mut c = Cursor {
        buf: body,
        remaining: (header.total_record_length as usize).checked_sub(X_LOG_RECORD_HEADER_SIZE)?,
    };
    let mut record = XLogRecord {
        header,
        ..Default::default()
    };
    let mut last_rel = None;
    let mut next_block_id = 0;
    while c.remaining > 0 {
        match c.u8()? {
            XLR_BLOCK_ID_DATA_SHORT => {
                record.main_data_len = c.u8()?.into();
                c.reserve(record.main_data_len as usize)?;
            }
            XLR_BLOCK_ID_DATA_LONG => {
                record.main_data_len = c.u32()?;
                c.reserve(record.main_data_len as usize)?;
            }
            XLR_BLOCK_ID_ORIGIN => record.origin = c.u16()?,
            XLR_BLOCK_ID_TOPLEVEL_XID => record.toplevel_xid = c.u32()?,
            id if id <= XLR_MAX_BLOCK_ID && id >= next_block_id => {
                next_block_id = id + 1;
                let flags = c.u8()?;
                let data_length = c.u16()?;
                if (flags & BKPBLOCK_HAS_DATA != 0) != (data_length != 0) {
                    return None;
                }
                c.reserve(data_length.into())?;
                if flags & BKP_BLOCK_HAS_IMAGE != 0 {
                    let image_length = image_header(&mut c, page_magic)?;
                    c.reserve(image_length.into())?;
                }
                let rel = if flags & BKPBLOCK_SAME_REL != 0 {
                    last_rel?
                } else {
                    RelFileNode {
                        spc_node: c.u32()?,
                        db_node: c.u32()?,
                        rel_node: c.u32()?,
                    }
                };
                c.u32()?; // block number
                last_rel = Some(rel);
                if !block(rel) {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(record)
}

/// Image length of a consistent block image header
fn image_header(c: &mut Cursor<'_>, page_magic: u16) -> Option<u16> {
    let mut h = XLogRecordBlockImageHeader {
        image_length: c.u16()?,
        hole_offset: c.u16()?,
        hole_length: 0,
        info: c.u8()?,
    };
    let compressed = h.is_compressed(page_magic);
    let has_hole = h.has_hole();
    h.hole_length = match (compressed, has_hole) {
        (true, true) => c.u16()?,
        (true, false) => 0,
        (false, _) => BLOCK_SIZE.saturating_sub(h.image_length),
    };
    let hole_ok = if has_hole {
        h.hole_offset != 0 && h.hole_length != 0 && h.image_length != BLOCK_SIZE
    } else {
        h.hole_offset == 0 && h.hole_length == 0
    };
    let length_ok = if compressed {
        h.image_length != BLOCK_SIZE
    } else {
        has_hole || h.image_length == BLOCK_SIZE
    };
    (hole_ok && length_ok).then_some(h.image_length)
}
