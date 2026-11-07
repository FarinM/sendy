use anyhow::{bail, ensure};

pub const VERSION_BYTE: u8 = 0x81;
pub const MAX_TX_SIZE: usize = 4096;

pub fn signatures(wire: &[u8]) -> anyhow::Result<&[[u8; 64]]> {
    ensure!(wire.len() <= MAX_TX_SIZE, "v1 transaction too large");
    let mut pos = 0;
    let header = take(wire, &mut pos, 42)?;
    ensure!(header[0] == VERSION_BYTE, "unsupported transaction version");
    let num_signatures = header[1] as usize;
    let num_keys = header[41] as usize;
    let num_instructions = header[40] as usize;
    ensure!(
        (1..=12).contains(&num_signatures),
        "invalid v1 signature count"
    );
    ensure!(header[2] < header[1], "message has no writable signer");
    ensure!(
        num_keys <= 64 && num_signatures + header[3] as usize <= num_keys,
        "invalid v1 account key count"
    );
    ensure!(num_instructions <= 64, "too many v1 instructions");

    let mask = u32::from_le_bytes(header[4..8].try_into().unwrap());
    ensure!(
        mask & !0x1f == 0 && matches!(mask & 3, 0 | 3),
        "invalid v1 transaction config mask"
    );
    let (account_keys, _) = take(wire, &mut pos, num_keys * 32)?.as_chunks::<32>();
    for (index, key) in account_keys.iter().enumerate() {
        ensure!(
            !account_keys[..index].contains(key),
            "duplicate v1 account key"
        );
    }

    let config = take(wire, &mut pos, mask.count_ones() as usize * 4)?;
    if mask & 0x10 != 0 {
        let heap_size = u32::from_le_bytes(config[config.len() - 4..].try_into().unwrap());
        ensure!(
            (32 * 1024..=256 * 1024).contains(&heap_size) && heap_size % 1024 == 0,
            "invalid v1 heap size"
        );
    }

    let (instruction_headers, _) = take(wire, &mut pos, num_instructions * 4)?.as_chunks::<4>();
    for header in instruction_headers {
        ensure!(
            header[0] != 0 && (header[0] as usize) < num_keys,
            "invalid instruction program id index"
        );
        let accounts = take(wire, &mut pos, header[1] as usize)?;
        ensure!(
            accounts.iter().all(|&index| (index as usize) < num_keys),
            "instruction account index is out of bounds"
        );
        let data_len = u16::from_le_bytes([header[2], header[3]]) as usize;
        take(wire, &mut pos, data_len)?;
    }
    let (signatures, _) = take(wire, &mut pos, num_signatures * 64)?.as_chunks::<64>();
    ensure!(pos == wire.len(), "trailing transaction bytes");

    Ok(signatures)
}

fn take<'a>(wire: &'a [u8], pos: &mut usize, len: usize) -> anyhow::Result<&'a [u8]> {
    let end = *pos + len;
    let Some(value) = wire.get(*pos..end) else {
        bail!("truncated v1 transaction");
    };
    *pos = end;
    Ok(value)
}
