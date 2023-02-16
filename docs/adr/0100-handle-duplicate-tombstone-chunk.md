# 100. Handle duplicate TombStone Chunks

 Date : 2023-02-16
 
## Status

Accepted

## Context

All Tombstone Chunks refer to the same singleton but are held by multiple and different chunkset or columns.
Currently, the base store of MAC is the Chunks store with fields related to the chunk as key fields.

The current DuplicateKeyHandler, called ChunkRecordHandler, handles 3 use cases :
- If both records come from the same partition, we do nothing
- If the previous record is held by multiple partitions, we return the previous record
- If both records have different parents, one of them should have a default value on the parent related fields (null for instance) or we throw. We define different parent with 3 fields :
  - The dictionary id of the parent of the chunk
  - The reference id of the parent of the chunk
  - The index id if the parent of the chunk

We then merge both records by keeping the non default values

Thus, if a user has discarded multiple Chunks, we run into a duplicate ID issue with the current MAC setup as different parts of the Cube will export the same Tombstone Chunk.

## Decision

The current ChunkRecordHandler has some dead code and handles unnecessary use cases.

We keep the sanity check in case two records for the same chunk with different parents happens to be exported.

Concerning TombstoneChunk, we decided to only keep one of the record and ignore the others as the impact on the memory footprint is insignificant

## Consequences

We do not perfectly report TombstoneChunks but it is not what the users want to know when they use MAC