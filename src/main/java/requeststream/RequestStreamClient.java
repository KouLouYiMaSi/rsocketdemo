package requeststream;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

public class RequestStreamClient {
    public static void main(String[] args) {
        RSocket socket =
                RSocketConnector.connectWith(TcpClientTransport.create("localhost", 7000)).block();

        // take（10）只取10条流数据
        socket
                .requestStream(DefaultPayload.create("Hello"))
                .map(Payload::getDataUtf8)
                .doOnNext(msg -> System.out.println(msg))
                .take(10)
                .then()
                .doFinally(signalType -> socket.dispose())
                .then()
                .block();
    }
}
