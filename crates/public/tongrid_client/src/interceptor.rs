use crate::errors::{TongridApiError, TongridApiResult};
use std::str::FromStr;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};

#[derive(Clone)]
pub struct TongridClientInterceptor {
    client_id: MetadataValue<tonic::metadata::Ascii>,
}

impl TongridClientInterceptor {
    pub fn new(client_id: &str) -> TongridApiResult<Self> {
        let client_id = match MetadataValue::from_str(client_id) {
            Ok(v) => v,
            Err(err) => {
                let err_str = format!("Invalid client_id for interceptor: {err}");
                return Err(TongridApiError::InvalidArguments(err_str));
            }
        };
        Ok(Self { client_id })
    }
}

impl Interceptor for TongridClientInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("x-client-id", self.client_id.clone());
        Ok(request)
    }
}
