package duplex;

import static io.rsocket.SocketAcceptor.forRequestStream;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;

import java.time.Duration;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class DuplexClient {

    public static void main(String[] args) {

        RSocketServer.create((setup, rsocket) -> {
            rsocket.requestStream(DefaultPayload.create("Hello-Bidi"))
                    .map(Payload::getDataUtf8)
                    .doOnNext(msg-> System.out.println(msg))
                    .subscribe();
            return Mono.just(new RSocket() {
            });
        })
                .bind(TcpServerTransport.create("localhost", 7000))
                .subscribe();

        RSocket rsocket = RSocketConnector.create().acceptor(
                forRequestStream(payload -> Flux.interval(Duration.ofSeconds(1))
                        .map(aLong -> DefaultPayload.create("Bi-di Response => " + aLong))))
                .connect(TcpClientTransport.create("localhost", 7000))
                .doOnRequest(msg-> System.out.println(msg))
                .block();

        rsocket.onClose().block();
    }
}