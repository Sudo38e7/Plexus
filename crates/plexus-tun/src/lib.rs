use std::io;

pub trait TunDevice: Send + Sync {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;
    fn write(&mut self, buf: &[u8]) -> io::Result<usize>;
}

pub struct TunConfig { pub name: String, pub mtu: u16, pub address: String }

pub fn open(_config: &TunConfig) -> io::Result<Box<dyn TunDevice>> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "TUN not implemented yet"))
}