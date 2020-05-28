package backpressure;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.lease.Leases;
import io.rsocket.lease.MissingLeaseException;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Slf4j
public class LeaseExample {

    private static final String SERVER_TAG = "server";
    private static final String CLIENT_TAG = "client";

    public static void main(String[] args) {
        // Queue for incoming messages represented as Flux
        // Imagine that every fireAndForget that is pushed is processed by a worker
        int queueCapacity = 50;
        BlockingQueue<String> messagesQueue = new ArrayBlockingQueue<>(queueCapacity);
        // emulating a worker that process data from the queue
        Thread workerThread =
                new Thread(
                        () -> {
                            try {
                                while (!Thread.currentThread().isInterrupted()) {
                                    String message = messagesQueue.take();
                                    System.out.println("Process message {}" + message);
                                    Thread.sleep(500); // emulating processing
                                }
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });
        workerThread.start();
        CloseableChannel server = getFireAndForgetServer(messagesQueue, workerThread);

        LeaseReceiver receiver = new LeaseReceiver(CLIENT_TAG);
        RSocket clientRSocket =
                RSocketConnector.create()
                        .lease(() -> Leases.create().receiver(receiver))
                        .connect(TcpClientTransport.create(server.address()))
                        .block();

        Objects.requireNonNull(clientRSocket);
        // generate stream of fnfs
        Flux.generate(() -> 0L, (state, sink) -> {
            sink.next(state);
            return state + 1;
        })
                // here we wait for the first lease for the responder side and start execution
                // on if there is allowance
                .delaySubscription(receiver.notifyWhenNewLease().then())
                .concatMap(tick -> {
                            System.out.println("Requesting FireAndForget({})" + tick);
                            return Mono.defer(() -> clientRSocket.fireAndForget(ByteBufPayload.create("" + tick)))
                                    .retryWhen(Retry.indefinitely()
                                                    // ensures that error is the result of missed lease
                                                    .filter(t -> t instanceof MissingLeaseException)
                                                    .doBeforeRetryAsync(
                                                            rs -> {
                                                                // here we create a mechanism to delay the retry until
                                                                // the new lease allowance comes in.
                                                                System.out.println("Ran out of leases {}" + rs);
                                                                return receiver.notifyWhenNewLease().then();
                                                            }));
                        })
                .blockLast();
        clientRSocket.onClose().block();
        server.dispose();
    }

    /**
     * 收到fireAndForget消息之后让消息入队。
     * 启动租约机制，5秒有效期和队列剩余容量可供请求
     *
     * @param messagesQueue
     * @param workerThread
     *
     * @return
     */
    private static CloseableChannel getFireAndForgetServer(BlockingQueue<String> messagesQueue, Thread workerThread) {
        CloseableChannel server =
                RSocketServer.create((setup, sendingSocket) ->
                        Mono.just(new RSocket() {
                            @Override
                            public Mono<Void> fireAndForget(Payload payload) {
                                // add element. if overflows errors and terminates execution
                                // specifically to show that lease can limit rate of fnf requests in
                                // that example
                                try {
                                    if (!messagesQueue.offer(payload.getDataUtf8())) {
                                        System.out.println("Queue has been overflowed. Terminating execution");
                                        sendingSocket.dispose();
                                        workerThread.interrupt();
                                    }
                                } finally {
                                    payload.release();
                                }
                                return Mono.empty();
                            }
                        }))
                        .lease(() -> Leases.create().sender(new LeaseCalculator(SERVER_TAG, messagesQueue)))
                        .bindNow(TcpServerTransport.create("localhost", 7000));
        return server;
    }

}