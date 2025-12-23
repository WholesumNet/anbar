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
    const CHUNK_SIZE: usize = 256_000;

    pub fn new() -> Self {
        Self {
            blobs: HashMap::new(),
            incomplete_blobs: HashMap::new(),
        }
    }


    pub fn get_blob(&self, hash: u128) -> Option<Vec<u8>> {
        if let Some(blob) = self.blobs.get(&hash) {
            Some(blob.data.clone())
        } else {
            None
        }
    }
    
    fn chunk(&mut self, data: &[u8]) -> Vec<BlobChunk> {
        let num_chunks = data.len() / Self::CHUNK_SIZE;
        let mut chunks: Vec<BlobChunk> = Vec::with_capacity(num_chunks);
        for i in 0..=num_chunks {
            let start_index = i * Self::CHUNK_SIZE;
            let end_index = std::cmp::min(data.len(), (i + 1) * Self::CHUNK_SIZE) - 1;
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

    pub fn store(&mut self, data: Vec<u8>) -> u128 {
        let hash = xxh3_128(&data);
        let chunks = self.chunk(&data);
        let blob = Blob {
            data: data,
            chunks: chunks,
        };
        self.blobs.insert(hash, blob);
        hash
    }

    pub fn is_blob_complete(&self, hash: u128) -> bool {
        self.blobs.contains_key(&hash)
    }

    pub fn get_num_chunks(&self, hash: u128) -> Option<usize> {
        if let Some(blob) = self.blobs.get(&hash) {
            Some(blob.chunks.len())
        } else {
            warn!("No such blob to get the number of chunks: `{hash}`.");
            None            
        }        
    }

    pub fn get_chunk(
        &self,
        hash: u128,
        index: usize
    ) -> Option<(Vec<u8>, u128)> {
        if let Some(blob) = self.blobs.get(&hash) {
            if let Some(chunk) = blob.chunks.get(index) {
                Some((
                    blob.data.get(chunk.start_index..=chunk.end_index).unwrap().to_vec(),
                    chunk.hash
                ))
            } else {
                warn!(
                    "Requested chunk index(`{}`) of blob(`{}`) is out of range.",
                    index,
                    hash
                );
                None                
            }
        } else {
            warn!("No such blob(`{hash}`) to get chunk of.");
            None           
        }          
    }

    pub fn add_incomplete_blob(
        &mut self,
        hash: u128,
        num_chunks: usize
    ) -> bool {
        if self.blobs.contains_key(&hash) {
            warn!("Blob(`{hash}`) is already complete.");
            return false
        }
        if self.incomplete_blobs.contains_key(&hash) {
            warn!("Incomplete blob(`{hash}`) already exists.");
            return false
        }  
        self.incomplete_blobs.insert(
            hash,
            IncompleteBlob {
                num_expected_chunks: num_chunks,
                chunks: BTreeMap::new(), 
            }
        );

        true
    }

    pub fn add_blob_chunk(
        &mut self,
        blob_hash: u128,
        index: usize,
        chunk_data: Vec<u8>,
        chunk_hash: u128
    ) { 
        if !self.incomplete_blobs.contains_key(&blob_hash) {
            warn!(
                "No such incomplete blob: `{}`",
                blob_hash
            );
            return
        }

        let incomplete_blob = self.incomplete_blobs.get_mut(&blob_hash).unwrap();
        if incomplete_blob.chunks.contains_key(&index) {
            warn!(
                "Chunk already exists at `{}th` index.",
                index
            );
            return
        }
        if index >= incomplete_blob.num_expected_chunks {
            warn!(
                "Received out of bounds chunk for index `{}`. It should be less than `{}`.",
                index,
                incomplete_blob.num_expected_chunks
            );
            return
        }
        //@ temporary
        let calculated_hash = xxh3_128(&chunk_data);
        if calculated_hash != chunk_hash {            
            warn!(
                "Chunk is corrupted. Expected `{}` but got `{}`.",
                chunk_hash,
                calculated_hash
            );
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
        let incomplete_blob = self.incomplete_blobs.remove(&hash).unwrap();        
        let mut data = Vec::with_capacity(
            incomplete_blob.chunks.len() * Self::CHUNK_SIZE
        );
        for mut chunk in incomplete_blob.chunks.into_values() {
            data.append(&mut chunk);            
        }
        let reconstructed_hash = xxh3_128(&data);
        if hash != reconstructed_hash {
            warn!(
                "Error in blob(`{}`) reconstruction. Hash mismatch, `{}`.",
                hash,
                reconstructed_hash
            );
            //@ wtd here?
            return
        }
        info!("Blob(`{hash}`) is reconstructed with success.");
        self.store(data);
    }

    pub fn get_next_blob_chunk_index(&self, hash: u128) -> Option<usize> {
        //@ what about out of order chunks?
        if let Some(incomplete_blob) = self.incomplete_blobs.get(&hash) {
            Some(incomplete_blob.chunks.len())
        } else {
            None
        }
    }

}
