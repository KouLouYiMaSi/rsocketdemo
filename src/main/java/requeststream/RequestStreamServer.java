package requeststream;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;

public class RequestStreamServer {
    public static void main(String[] args) throws InterruptedException {
        // 收到请求之后返回消息流
        RSocketServer.create(
                SocketAcceptor.forRequestStream(payload -> Flux.interval(Duration.ofMillis(100))
                        .map(aLong -> DefaultPayload.create("服务端返回消息: " + aLong))))
                .bind(TcpServerTransport.create("localhost", 7000))
                .subscribe();

        TimeUnit.MINUTES.sleep(10);
    }
}
