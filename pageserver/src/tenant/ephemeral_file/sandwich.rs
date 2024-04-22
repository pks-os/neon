use crate::{page_cache::PAGE_SZ, virtual_file::VirtualFile};

use super::size_tracker_borrowed;

pub struct Sandwich {
    sandwich: size_tracker_borrowed::Writer<{ Self::TAIL_SZ }>,
}

pub enum ReadResult<'a> {
    NeedsReadFromVirtualFile { virtual_file: &'a VirtualFile },
    ServedFromZeroPaddedMutableTail { buffer: &'a [u8; PAGE_SZ] },
}

impl Sandwich {
    const TAIL_SZ: usize = 64 * 1024;

    pub fn new(file: VirtualFile) -> Self {
        let size_borrowed_tracker = size_tracker_borrowed::Writer::new(file);
        Self {
            sandwich: size_borrowed_tracker,
        }
    }

    pub(crate) fn as_inner_virtual_file(&self) -> &VirtualFile {
        self.sandwich.as_inner_virtual_file()
    }

    pub async fn write_all_borrowed(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.sandwich.write_all_borrowed(buf).await
    }

    pub fn bytes_written(&self) -> u64 {
        self.sandwich.buffered_offset()
    }

    pub(crate) async fn read_blk(&self, blknum: u32) -> Result<ReadResult, std::io::Error> {
        let buffered_offset = self.sandwich.buffered_offset();
        let flushed_offset = self.sandwich.flushed_offset();
        assert!(buffered_offset >= flushed_offset);
        let read_offset = (blknum as u64) * (PAGE_SZ as u64);

        assert_eq!(
            flushed_offset % (PAGE_SZ as u64),
            0,
            "we need this in the logic below, because it assumes the page isn't spread across flushed part and in-memory buffer"
        );

        if read_offset < flushed_offset {
            assert!(
                read_offset + (PAGE_SZ as u64) <= flushed_offset,
                "this impl can't deal with pages spread across flushed & buffered part"
            );
            Ok(ReadResult::NeedsReadFromVirtualFile {
                virtual_file: self.as_inner_virtual_file(),
            })
        } else {
            let read_until_offset = read_offset + (PAGE_SZ as u64);
            if !(0..buffered_offset).contains(&read_until_offset) {
                // The blob_io code relies on the reader allowing reads past
                // the end of what was written, up to end of the current PAGE_SZ chunk.
                // This is a relict of the past where we would get a pre-zeroed page from the page cache.
                //
                // DeltaLayer probably has the same issue, not sure why it needs no special treatment.
                let nbytes_past_end = read_until_offset.checked_sub(buffered_offset).unwrap();
                if nbytes_past_end >= (PAGE_SZ as u64) {
                    // TODO: treat this as error. Pre-existing issue before this patch.
                    panic!(
                        "return IO error: read past end of file: read=0x{read_offset:x} buffered=0x{buffered_offset:x} flushed=0x{flushed_offset}"
                    )
                }
            }
            let buffer: &[u8; Self::TAIL_SZ] =
                self.sandwich.inspect_buffer().as_zero_padded_slice();
            let read_offset_in_buffer = read_offset
                .checked_sub(flushed_offset)
                .expect("would have taken `if` branch instead of this one");

            let read_offset_in_buffer = usize::try_from(read_offset_in_buffer).unwrap();
            let page = &buffer[read_offset_in_buffer..(read_offset_in_buffer + PAGE_SZ)];
            Ok(ReadResult::ServedFromZeroPaddedMutableTail {
                buffer: page
                    .try_into()
                    .expect("the slice above got it as page-size slice"),
            })
        }
    }
}
