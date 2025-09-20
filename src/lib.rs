use std::collections::{HashMap, BTreeMap};
use xxhash_rust::xxh3::xxh3_128;
use log::{info, warn};

// the blob that gets pulled from other peers
#[derive(Debug, Clone)]
pub struct IncompleteBlob {
    // the number of chunks to receive 
    pub num_expected_chunks: usize,

    pub chunks: BTreeMap<usize, Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct Blob {
    data: Vec<u8>,

    chunks: Vec<BlobChunk>,
}

#[derive(Debug, Clone)]
struct BlobChunk {
    // start byte    
    start_index: usize,

    // end byte
    end_index: usize,

    // chunk hash
    hash: u128,
}

// where blobs are stored 
#[derive(Debug, Clone)]
pub struct BlobStore {
    // complete blobs
    blobs: HashMap<u128, Blob>,

    incomplete_blobs: HashMap<u128, IncompleteBlob>,
}

impl BlobStore {
    pub fn new() -> Self {
        Self {
            blobs: HashMap::new(),
            incomplete_blobs: HashMap::new(),
        }
    }

    // chunk and store a blob
    pub fn store(&mut self, data: Vec<u8>) {
        let hash = xxh3_128(&data);
        let chunks = self.chunk(&data);
        let blob = Blob {
            data: data,
            chunks: chunks,
        };
        self.blobs.insert(hash, blob);
    }

    pub fn get_blob(&self, hash: u128) -> Option<Vec<u8>> {
        if let Some(blob) = self.blobs.get(&hash) {
            Some(blob.data.clone())
        } else {
            None
        }
    }
    
    // chunk a blob
    fn chunk(&mut self, data: &Vec<u8>) -> Vec<BlobChunk> {
        let chunk_size = 256 * 1_000;
        let mut chunks: Vec<BlobChunk> = Vec::new();
        for i in 0..=(data.len() / chunk_size) {
            let start_index = i * chunk_size;
            let end_index = std::cmp::min(data.len(), (i + 1) * chunk_size) - 1;
            // info!("chunk {i}: `{start_index}`-`{end_index}`");
            let chunk = data.get(start_index..=end_index).unwrap();
            let hash = xxh3_128(chunk);
            chunks.push(BlobChunk {
                start_index: start_index,
                end_index: end_index,
                hash: hash,
            });
        }
        chunks
    }

    pub fn is_blob_complete(&self, hash: u128) -> bool {
        self.blobs.contains_key(&hash)
    }

    // get the number of chunks
    pub fn get_blob_info(&self, hash: u128) -> Option<usize> {
        if !self.blobs.contains_key(&hash) {
            warn!("No such blob to get info of: `{hash}`.");
            None
        } else {
            Some(self.blobs.get(&hash).unwrap().chunks.len())
        }
    }

    pub fn get_chunk(
        &self,
        hash: u128,
        index: usize
    ) -> Option<(Vec<u8>, u128)> {
        if !self.blobs.contains_key(&hash) {
            warn!("No such blob(`{hash}`) to get chunk of.");
            return None
        }
        let blob = self.blobs.get(&hash).unwrap();
        if index >= blob.chunks.len() {
            warn!("Chunk index(`{index}`) of blob(`{hash}`) is out of range.");
            return None
        } 
        let chunk = blob.chunks.get(index).unwrap();
        Some((
            blob.data.get(chunk.start_index..=chunk.end_index).unwrap().to_vec(),
            chunk.hash
        ))
    }

    pub fn add_incomplete_blob(&mut self, hash: u128) {
        if self.blobs.contains_key(&hash) {
            warn!("Blob(`{hash}`) is already complete.");
            return
        }
        if self.incomplete_blobs.contains_key(&hash) {
            warn!("Incomplete blob(`{hash}`) already exists.");
            return
        }
        self.incomplete_blobs.insert(
            hash,
            IncompleteBlob {
                num_expected_chunks: 0usize,
                chunks: BTreeMap::new(), 
            }
        );
    }

    pub fn add_blob_info(&mut self, hash: u128, num_chunks: usize) {        
        if !self.incomplete_blobs.contains_key(&hash) {
            warn!("No such blob to add info for: `{hash}`.");
            return
        }
        let incomplete_blob = self.incomplete_blobs.get_mut(&hash).unwrap();
        incomplete_blob.num_expected_chunks = num_chunks;
    }

    pub fn add_blob_chunk(
        &mut self,
        blob_hash: u128,
        index: usize,
        chunk_data: Vec<u8>,
        chunk_hash: u128
    ) {
        let calculated_hash = xxh3_128(&chunk_data);
        if calculated_hash != chunk_hash {            
            warn!("Chunk is corrupted: hash mistmatch: `{calculated_hash}` != `{chunk_hash}`");
            return
        }
        if !self.incomplete_blobs.contains_key(&blob_hash) {
            warn!("No such incomplete blob(`{blob_hash}`).");
            return
        }
        let incomplete_blob = self.incomplete_blobs.get_mut(&blob_hash).unwrap();
        if incomplete_blob.chunks.contains_key(&index) {
            warn!("Duplicate chunk, index: `{index}`.");
            return
        }
        if index >= incomplete_blob.num_expected_chunks {
            warn!("Out of bounds chunk, index: `{index}`.");
            return
        }
        incomplete_blob.chunks.insert(
            index,            
            chunk_data
        );
        if incomplete_blob.chunks.len() == incomplete_blob.num_expected_chunks {
            self.reconstruct_blob(blob_hash);
        }
    }

    fn reconstruct_blob(&mut self, hash: u128) {
        let incomplete_blob = self.incomplete_blobs.get_mut(&hash).unwrap();        
        let mut data = Vec::new();
        for chunk in incomplete_blob.chunks.values() {
            data.extend_from_slice(&chunk);            
        }
        let reconstructed_hash = xxh3_128(&data);
        if hash != reconstructed_hash {
            warn!("Error in blob(`{hash}`) reconstruction: hash mismatch.");
            //@ wtd here?
            return
        }
        info!("Blob(`{hash}`) reconstruction succeded.");
        self.incomplete_blobs.remove(&hash);
        self.store(data);
    }

    pub fn get_next_blob_chunk_index(&self, hash: u128) -> Option<usize> {
        if let Some(incomplete_blob) = self.incomplete_blobs.get(&hash) {
            Some(incomplete_blob.chunks.len())
        } else {
            None
        }
    }

}
