use crate::forwarding_layer::ForwardingLayer;
use crate::middleware::GrpcMiddleware;
use parking_lot::Mutex;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::ops::Deref;
use tokio::net::TcpListener;
use tonic::body::Body;
use tonic::codegen::http::Request;
use tonic::codegen::http::Response;
use tonic::server::NamedService;
use tonic::transport::server::Router;
use tonic::transport::Server;
use tonic_middleware::MiddlewareLayer;
use tonic_middleware::RequestInterceptorLayer;
use tonic_reflection::server::Builder;
use tower::util::Either;

type BaseLayers = MiddlewareLayer<GrpcMiddleware>;
type LayersWithForwarding =
    tower_layer::Stack<RequestInterceptorLayer<ForwardingLayer>, BaseLayers>;
type TonicRouter =
    Router<tower_layer::Stack<Either<LayersWithForwarding, BaseLayers>, tower_layer::Identity>>;
pub struct GrpcServer {
    address: SocketAddr,
    router: Mutex<Option<TonicRouter>>,
    listen_addr: Mutex<Option<SocketAddr>>,
}

impl GrpcServer {
    pub fn new(
        config: &crate::config::GrpcServerConfig,
        forwarding: Option<ForwardingLayer>,
        reflection_descriptor: Option<&[u8]>,
    ) -> anyhow::Result<Self> {
        let address = config.listen_address.parse()?;
        let max_concurrent_streams = config.max_concurrent_streams;
        let middleware = MiddlewareLayer::new(GrpcMiddleware::new());
        let reflection_service = reflection_descriptor
            .map(|descr| {
                Builder::configure()
                    .register_encoded_file_descriptor_set(descr)
                    .build_v1()
            })
            .transpose()?;

        let layer = match forwarding {
            Some(layer) => Either::Left(tower_layer::Stack::new(
                RequestInterceptorLayer::new(layer),
                middleware,
            )),
            None => Either::Right(middleware),
        };

        let router = Server::builder()
            .max_concurrent_streams(max_concurrent_streams)
            .layer(layer)
            .add_optional_service(reflection_service);

        let server = Self {
            address,
            router: Mutex::new(Some(router)),
            listen_addr: Mutex::new(None),
        };

        Ok(server)
    }

    pub async fn add_service<S>(&mut self, service: S) -> anyhow::Result<()>
    where
        S: tonic::codegen::Service<Request<Body>, Response = Response<Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        // Router::add_service consumes self; temporarily take ownership to replace the stored router.
        let cur_router = self
            .router
            .lock()
            .take()
            .ok_or_else(|| anyhow::anyhow!("gRPC server has already been started"))?;
        let new_router = cur_router.add_service(service);
        self.router.lock().replace(new_router);
        Ok(())
    }

    pub fn get_listening_port(&self) -> Option<SocketAddr> {
        *self.listen_addr.lock().deref()
    }

    pub async fn serve(&self) -> anyhow::Result<()> {
        let addr = self.address;
        let listener = TcpListener::bind(addr).await?;
        let listener_addr = listener.local_addr()?;

        let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let router = self
            .router
            .lock()
            .take()
            .ok_or_else(|| anyhow::anyhow!("gRPC server has already been started"))?;
        *self.listen_addr.lock() = Some(listener_addr);

        log::info!("Starting gRPC server on {listener_addr}");
        router.serve_with_incoming(stream).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio_test::assert_ok;

    #[tokio::test]
    async fn test_server_works() -> anyhow::Result<()> {
        let server = Arc::new(GrpcServer::new(
            &crate::config::GrpcServerConfig {
                listen_address: "127.0.0.1:0".into(),
                max_concurrent_streams: 50,
                forward: crate::config::ForwardConfig {
                    address: String::new(),
                    percentage: 0.0,
                    enabled: false,
                },
            },
            None,
            None,
        )?);

        // start server
        let server_scope = server.clone();
        tokio::spawn(async move {
            server_scope.serve().await?;
            Ok::<_, anyhow::Error>(())
        });

        // wait for server to start
        let start_time = tokio::time::Instant::now();
        let listen_addr = loop {
            match server.get_listening_port() {
                Some(addr) => break addr,
                None => tokio::time::sleep(Duration::from_millis(20)).await,
            }
            if start_time.elapsed() > Duration::from_secs(5) {
                panic!("Server did not start in 5 seconds");
            }
        };
        assert_ok!(TcpStream::connect(&listen_addr).await);
        Ok(())
    }
}
