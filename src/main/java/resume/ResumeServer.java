package resume;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

/**
 * 用于请求文件流，把请求到的文件写入到自己这边
 * 使用方法：
 * 1、使用socat工具对8001的tcp请求转发到8000上
 * <p>socat -d TCP-LISTEN:8001,fork,reuseaddr P:localhost:8000</p>
 * 2、执行RusumeServer
 * 3、执行ResumeClient
 * 4、查看文件是否写入到了根目录
 * 5、文件已经写过来就ctrl+c停止socat转发模拟网络断开，会打印断开日志
 */
@Slf4j
public class ResumeServer {

    public static void main(String[] args) throws InterruptedException {
        Resume resume = new Resume()
                .sessionDuration(Duration.ofMinutes(5))
                .retry(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .doBeforeRetry(s -> log.debug(s.toString())));

        RequestCodec codec = new RequestCodec();
        CloseableChannel server = RSocketServer.create(
                SocketAcceptor.forRequestStream(payload -> {
                    Request request = codec.decode(payload);
                    payload.release();
                    String fileName = request.getFileName();
                    int chunkSize = request.getChunkSize();
                    Flux<Long> ticks = Flux.interval(Duration.ofMillis(500)).onBackpressureDrop();
                    return Files.fileSource(fileName, chunkSize)
                            .map(DefaultPayload::create)
                            .zipWith(ticks, (p, tick) -> p);
                }))
                .resume(resume)
                .bind(TcpServerTransport.create("localhost", 8000))
                .block();
        TimeUnit.MINUTES.sleep(5);
    }

}