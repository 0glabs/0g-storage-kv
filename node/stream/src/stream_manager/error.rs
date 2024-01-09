use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Invalid stream data")]
    InvalidData,
    #[error("Stream read/write/access_control list too long")]
    ListTooLong,
    #[error("Only partial data are available")]
    PartialDataAvailable,
}
