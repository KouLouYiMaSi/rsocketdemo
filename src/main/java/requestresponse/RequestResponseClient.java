package requestresponse;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

public class RequestResponseClient {
    public static void main(String[] args) {
        RSocket socketClient =
                RSocketConnector.connectWith(TcpClientTransport.create("localhost", 7001)).block();

        // Payload create用于构建消息
        // doOnNext用于处理收到的连接上的信息
        for (int i = 0; i < 10; i++) {
            socketClient
                    .requestResponse(DefaultPayload.create("客户端消息 " + i))
                    .map(Payload::getDataUtf8)
                    .onErrorReturn("error")
                    .doOnNext(s -> System.out.println(s))
                    .block();
        }

        socketClient.dispose();
    }
}
