package backpressure;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

import io.rsocket.lease.Lease;
import io.rsocket.lease.LeaseStats;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

/**
 * This is a class responsible for making decision on whether Responder is ready to receive new
 * FireAndForget or not base in the number of messages enqueued. <br>
 * In the nutshell this is responder-side rate-limiter logic which is created for every new
 * connection.<br>
 * In real-world projects this class has to issue leases based on real metrics
 */
@Slf4j
public class LeaseCalculator implements Function<Optional<LeaseStats>, Flux<Lease>> {
    final String tag;
    final BlockingQueue<?> queue;

    public LeaseCalculator(String tag, BlockingQueue<?> queue) {
        this.tag = tag;
        this.queue = queue;
    }

    @Override
    public Flux<Lease> apply(Optional<LeaseStats> leaseStats) {
        log.info("{} stats are {}", tag, leaseStats.isPresent() ? "present" : "absent");
        Duration ttlDuration = Duration.ofSeconds(5);
        // The interval function is used only for the demo purpose and should not be
        // considered as the way to issue leases.
        // For advanced RateLimiting with Leasing
        // consider adopting https://github.com/Netflix/concurrency-limits#server-limiter
        // 每2秒发送租约，租约内容为队列容量和5秒有效期
        return Flux.interval(Duration.ZERO, ttlDuration.dividedBy(2))
                .handle((__, sink) -> {
                    // put queue.remainingCapacity() + 1 here if you want to observe that app is
                    // terminated  because of the queue overflowing
                    int requests = queue.remainingCapacity();
                    // reissue new lease only if queue has remaining capacity to
                    // accept more requests
                    if (requests > 0) {
                        long ttl = ttlDuration.toMillis();
                        sink.next(Lease.create((int) ttl, requests));
                    }
                });
    }
}