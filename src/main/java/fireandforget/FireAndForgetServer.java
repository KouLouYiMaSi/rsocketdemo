package fireandforget;

import java.util.concurrent.TimeUnit;

import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import reactor.core.publisher.Mono;

public class FireAndForgetServer {
    public static void main(String[] args) throws InterruptedException {
        RSocketServer.create(SocketAcceptor.forFireAndForget(payload -> {
            System.out.println(payload.getDataUtf8());
            return Mono.empty();
        })).bind(TcpServerTransport.create("localhost", 7001))
                .doOnSuccess(msg -> System.out.println(Thread.currentThread().getName()))
                .doOnNext(msg -> System.out.println(msg))
                .doOnError(msg -> System.out.println(msg))
                .block();
        TimeUnit.MINUTES.sleep(10);
    }
}
