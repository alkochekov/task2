import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HandlerImpl implements Handler {

    private final Client client;
    private final ExecutorService executor;

    public HandlerImpl(Client client) {
        this.client = client;
        this.executor = Executors.newCachedThreadPool();
    }

    @Override
    public Duration timeout() {
        return Duration.ofSeconds(1);
    }

    @Override
    public void performOperation() {
        while (true) {
            Event event = client.readData();
            for (Address recipient : event.recipients()) {
                executor.submit(() -> {
                    Result result;
                    do {
                        result = client.sendData(recipient, event.payload());
                        if (result == Result.REJECTED) {
                            try {
                                Thread.sleep(timeout().toMillis());
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    } while (result == Result.REJECTED);
                });
            }
        }
    }

    public void shutdown() throws InterruptedException {
        executor.shutdown();
        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }
    }
}
