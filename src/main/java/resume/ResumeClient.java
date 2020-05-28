package resume;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.Resume;
import io.rsocket.transport.netty.client.TcpClientTransport;
import lombok.extern.slf4j.Slf4j;
import reactor.util.retry.Retry;

@Slf4j
public class ResumeClient {
    private static final int PREFETCH_WINDOW_SIZE = 4;

    public static void main(String[] args) throws InterruptedException {
        RequestCodec codec = new RequestCodec();
        Resume resume = new Resume()
                .sessionDuration(Duration.ofMinutes(5))
                .retry(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .doBeforeRetry(s -> log.debug(s.toString())));
        RSocket client = RSocketConnector.create()
                .resume(resume)
                .connect(TcpClientTransport.create("localhost", 8001))
                .block();

        client.requestStream(codec.encode(new Request(16, "lorem.txt")))
                .subscribe(Files.fileSink("lorem_output.txt", PREFETCH_WINDOW_SIZE));

        TimeUnit.MINUTES.sleep(6);
    }
}
