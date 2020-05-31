package backpressure;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketServer;
import io.rsocket.lease.Leases;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
public class LeaseServer {

    private static final String SERVER_TAG = "server";

    public static void main(String[] args) throws InterruptedException {
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
                                    System.out.println("消费者线程处理消息：" + message);
                                    Thread.sleep(100000); // emulating processing
                                }
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });
        workerThread.start();
        CloseableChannel server = getFireAndForgetServer(messagesQueue, workerThread);
        TimeUnit.MINUTES.sleep(10);
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