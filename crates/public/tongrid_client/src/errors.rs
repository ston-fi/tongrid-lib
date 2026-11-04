use thiserror::Error;
use tokio::time::error::Elapsed;

#[derive(Debug, Error)]
pub enum TongridApiError {
    #[error("TongridApiNetError: {0}")]
    NetError(String),
    #[error("TongridApiInvalidArgument: {0}")]
    InvalidArguments(String),
    #[error("TongridApiNotFoundError: {0}")]
    NotFound(String),
    #[error("TongridApiServerError: {0}")]
    ServerError(#[source] Box<tonic::Status>),
    #[error("TongridApiUnexpectedResponse: {0}")]
    UnexpectedResponse(String),
    #[error("CustomError: {0}")]
    CustomError(String),
    #[error("Timeout: {0}")]
    TimeoutError(#[source] Elapsed),
    #[error("rcv error: {0}")]
    RecvError(#[source] tokio::sync::watch::error::RecvError),
    #[error("Internal error: {0}")]
    TonicError(#[source] Box<tonic::transport::Error>),
}

pub type TongridApiResult<T> = Result<T, TongridApiError>;

impl From<tonic::transport::Error> for TongridApiError {
    fn from(err: tonic::transport::Error) -> Self {
        TongridApiError::TonicError(Box::new(err))
    }
}

impl From<tonic::Status> for TongridApiError {
    fn from(err: tonic::Status) -> Self {
        TongridApiError::ServerError(Box::new(err))
    }
}
