package fireandforget;

import java.util.concurrent.TimeUnit;

import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

public class FireAndForgetClient {
    public static void main(String[] args) throws InterruptedException {
        RSocket socketClient =
                RSocketConnector.connectWith(TcpClientTransport.create("localhost", 7001)).block();
        for (int i = 1; i < 11; i++) {
            TimeUnit.SECONDS.sleep(1);
            socketClient.fireAndForget(DefaultPayload.create("客户端消息FireAndForget"+i)).block();
        }
    }
}
