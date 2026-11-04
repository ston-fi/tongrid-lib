use thiserror::Error;
use ton::errors::TonError;
use ton::ton_core::errors::TonCoreError;
use tonic::{Code, Status};

#[derive(Debug, Error)]
pub enum GrpcServiceError {
    #[error("{0}")]
    InvalidArgument(String),

    #[error("{0}")]
    ResourceExhausted(String),

    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    TimedOut(String),

    #[error("{0}")]
    InternalError(String),

    #[error("{0}")]
    TonError(#[from] TonError),
    #[error("{0}")]
    TonCoreError(#[from] TonCoreError),
}

impl From<anyhow::Error> for GrpcServiceError {
    fn from(error: anyhow::Error) -> Self {
        GrpcServiceError::InternalError(error.to_string())
    }
}

impl From<GrpcServiceError> for Status {
    fn from(err: GrpcServiceError) -> Self {
        let code = match err {
            GrpcServiceError::InvalidArgument(_) => Code::InvalidArgument,
            GrpcServiceError::ResourceExhausted(_) => Code::ResourceExhausted,
            GrpcServiceError::NotFound(_) => Code::NotFound,
            GrpcServiceError::TimedOut(_) => Code::DeadlineExceeded,
            GrpcServiceError::InternalError(_) => Code::Internal,

            // tonlib typed errors
            GrpcServiceError::TonError(_) => Code::Internal,
            GrpcServiceError::TonCoreError(_) => Code::Internal,
        };

        // Use the Display implementation for the error message
        Status::new(code, err.to_string())
    }
}

impl From<std::time::SystemTimeError> for GrpcServiceError {
    fn from(error: std::time::SystemTimeError) -> Self {
        Self::InternalError(error.to_string())
    }
}
