include!(concat!(env!("OUT_DIR"), "/messages.rs"));

pub mod system {
    include!("system.proprdb.pb.rs");
}
pub mod extra {
    include!("rust.proprdb.pb.rs");
}

impl generatedtest::example::Person {
    pub fn valid(&self) -> proprdb_runtime::Result<()> {
        if self.name.is_empty() {
            return Err(proprdb_runtime::Error::Invalid("name is empty".into()));
        }
        Ok(())
    }
}
