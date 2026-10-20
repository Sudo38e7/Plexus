//! FEC через Reed-Solomon erasure codes.
//! K data-шардов + M parity-шардов на группу → переживает до M потерь в группе.

use reed_solomon_erasure::galois_8::ReedSolomon;

#[derive(Debug, thiserror::Error)]
pub enum FecError {
    #[error("reed-solomon init failed: {0}")]
    Init(String),
    #[error("reed-solomon encode failed: {0}")]
    Encode(String),
    #[error("shard count mismatch: expected {expected}, got {got}")]
    ShardCount { expected: usize, got: usize },
}

pub struct FecCodec {
    rs: ReedSolomon,
    data_shards: usize,
    parity_shards: usize,
}

impl FecCodec {
    pub fn new(data_shards: usize, parity_shards: usize) -> Result<Self, FecError> {
        let rs = ReedSolomon::new(data_shards, parity_shards)
            .map_err(|e| FecError::Init(format!("{:?}", e)))?;
        Ok(Self { rs, data_shards, parity_shards })
    }

    pub fn data_shards(&self) -> usize { self.data_shards }
    pub fn parity_shards(&self) -> usize { self.parity_shards }
    pub fn total_shards(&self) -> usize { self.data_shards + self.parity_shards }

    /// Принимает K data-шардов (все одного размера), возвращает M parity-шардов.
    pub fn encode(&self, data: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, FecError> {
        if data.len() != self.data_shards {
            return Err(FecError::ShardCount { expected: self.data_shards, got: data.len() });
        }
        let shard_len = data.first().map(|s| s.len()).unwrap_or(0);
        if !data.iter().all(|s| s.len() == shard_len) {
            return Err(FecError::Encode("data shards must be equal length".into()));
        }

        let mut shards: Vec<Vec<u8>> = data.to_vec();
        shards.extend(std::iter::repeat(vec![0u8; shard_len]).take(self.parity_shards));
        self.rs.encode(&mut shards).map_err(|e| FecError::Encode(format!("{:?}", e)))?;
        Ok(shards.split_off(self.data_shards))
    }

    /// Принимает K+M слотов (некоторые None), восстанавливает недостающие на месте.
    /// Возвращает true, если восстановление успешно (или нечего восстанавливать).
    pub fn reconstruct(&self, shards: &mut [Option<Vec<u8>>]) -> bool {
        if shards.len() != self.total_shards() { return false; }
        if shards.iter().all(|s| s.is_some()) { return true; }
        self.rs.reconstruct(shards).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_then_recover_two_losses() {
        let k = 6;
        let m = 3;
        let codec = FecCodec::new(k, m).unwrap();
        let shard_len = 16;
        let data: Vec<Vec<u8>> = (0..k)
            .map(|i| (0..shard_len).map(|j| ((i * 7 + j * 13) & 0xff) as u8).collect())
            .collect();
        let parity = codec.encode(&data).unwrap();
        assert_eq!(parity.len(), m);

        let mut shards: Vec<Option<Vec<u8>>> = Vec::new();
        for d in &data { shards.push(Some(d.clone())); }
        for p in &parity { shards.push(Some(p.clone())); }

        // Теряем 2 data-шарда и 1 parity — восстановление должно быть возможно (потерь <= M)
        shards[1] = None;
        shards[4] = None;
        shards[k + 2] = None;

        assert!(codec.reconstruct(&mut shards));
        for i in 0..k {
            assert_eq!(shards[i].as_deref(), Some(&data[i][..]));
        }
    }

    #[test]
    fn recover_fails_when_too_many_lost() {
        let codec = FecCodec::new(4, 2).unwrap();
        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i as u8; 8]).collect();
        let parity = codec.encode(&data).unwrap();
        let mut shards: Vec<Option<Vec<u8>>> =
            data.iter().cloned().map(Some).chain(parity.iter().cloned().map(Some)).collect();
        // Теряем 3 data (больше M=2) — восстановление невозможно
        shards[0] = None;
        shards[1] = None;
        shards[2] = None;
        assert!(!codec.reconstruct(&mut shards));
    }
}