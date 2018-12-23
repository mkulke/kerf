use std::net::SocketAddrV4;
pub fn is_host_port(val: String) -> Result<(), String> {
    match val.parse::<SocketAddrV4>() {
        Ok(_) => Ok(()),
        _ => Err(String::from("wrong format")),
    }
}
