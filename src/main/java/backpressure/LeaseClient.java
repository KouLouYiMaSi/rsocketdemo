package backpressure;

import java.util.Objects;

import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.lease.Leases;
import io.rsocket.lease.MissingLeaseException;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class LeaseClient {

    private static final String CLIENT_TAG = "client";

    public static void main(String[] args) {
        LeaseReceiver receiver = new LeaseReceiver(CLIENT_TAG);
        RSocket clientRSocket =
                RSocketConnector.create()
                        .lease(() -> Leases.create().receiver(receiver))
                        .connect(TcpClientTransport.create("localhost", 7000))
                        .block();

        Objects.requireNonNull(clientRSocket);
        // generate stream of fnfs
        Flux.generate(() -> 0L, (state, sink) -> {
            // 给下游订阅者发送单个消息
            sink.next(state);
            return state + 1;
        })
                // 等待新的租约到来再继续执行下边的，不然就在这阻塞
                .delaySubscription(receiver.notifyWhenNewLease().then())
                // 新租约到来之后，flatten和order这些流的帧
                .concatMap(tick -> {
                    System.out.println("客户端发射消息" + tick);
                    // 有订阅者之后再创建mono
                    return Mono.defer(() -> clientRSocket.fireAndForget(ByteBufPayload.create("" + tick)))
                            // retry.indefinitely表示非立即重试，也就是说下一次重试没有确定时间
                            .retryWhen(Retry.indefinitely()
                                    // 只有在租约到期的错误的时候才开始等待新租约
                                    .filter(t -> t instanceof MissingLeaseException)
                                    // 执行重试之前的信号，也就是新的租约到来的时候才会重试
                                    .doBeforeRetryAsync(
                                            rs -> {
                                                // 在重试之前会阻塞，直到新的租约到来
                                                System.out.println("租约到期：" + rs);
                                                return receiver.notifyWhenNewLease().then();
                                            }));
                })
                .blockLast();
        clientRSocket.onClose().block();
    }
}
