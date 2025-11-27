package kite.datasource;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodySubscriber;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingQueue;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.Iterator;

/**
 * StreamHttpClient is a lightweight, dependency-free HTTP client that uses
 * Java's built-in java.net.http.HttpClient.
 *
 * This version uses a custom BodyHandler and BodySubscriber to integrate the
 * reactive-to-synchronous stream bridge (ByteBufferChunkStream) directly into
 * the client's network handling process, maximizing memory efficiency and
 * simplifying the internal streaming API.
 *
 * NOTE: This requires Java 21 or newer.
 */
public class StreamHttpClient implements AutoCloseable {

    // The standard Java 11+ HttpClient. No external dependencies needed.
    private final HttpClient httpClient;

    // The executor is kept for the HttpClient's internal tasks (like connection setup).
    private final ExecutorService internalExecutor;

    /**
     * Sentinel value used to signal an error from the producer thread to the consumer thread.
     * This zero-byte buffer prevents null from entering the BlockingQueue.
     */
    public static final ByteBuffer ERROR_SENTINEL = ByteBuffer.allocate(0);

    // Define the type for the internal queue which now holds lists of ByteBuffers
    private static final int QUEUE_CAPACITY = 10;
    private static final Class<List> QUEUE_TYPE = List.class;

    /**
     * Represents a context object to carry request-specific metadata.
     */
    public record Context(Map<String, Object> metadata) {
        public Context() {
            this(new HashMap<>());
        }

        public Context put(String key, Object value) {
            metadata.put(key, value);
            return this;
        }

        public Optional<Object> get(String key) {
            return Optional.ofNullable(metadata.get(key));
        }
    }

    /**
     * Sealed interface to represent the result of stream setup, allowing the caller
     * to pattern match on success (with the InputStream) or immediate failure.
     */
    public sealed interface StreamResult permits StreamResult.Success, StreamResult.Failure {
        /** Represents a successful stream initiation, providing the InputStream and the request Context. */
        record Success(InputStream inputStream, Context context) implements StreamResult {}
        /** Represents an error encountered during connection or header reading, along with the request Context. */
        record Failure(IOException error, Context context) implements StreamResult {}
    }


    /**
     * Initializes the StreamHttpClient.
     */
    public StreamHttpClient() {
        // 1. Create an internal Virtual Thread executor with custom naming (kite-vt-http-X)
        this.internalExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("kite-vt-http-", 0).factory()
        );

        // 2. Build the HttpClient, injecting the virtual thread executor for efficiency.
        this.httpClient = HttpClient.newBuilder()
                .executor(internalExecutor)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Executes an HTTP request and converts the reactive network stream into a
     * standard synchronous InputStream using a custom BodyHandler.
     *
     * @param request The complete HttpRequest object.
     * @param context The request context containing metadata.
     * @return A StreamResult containing either the InputStream or the error.
     */
    public StreamResult stream(HttpRequest request, Context context) {

        // 1. Get the custom BodyHandler which encapsulates the stream setup.
        var streamingHandler = CustomStreamingBodySubscriber.createStreamingHandler();

        try {
            // 2. Synchronously send the request. The body will be processed by our custom subscriber,
            // which immediately pushes the InputStream (our ByteBufferChunkStream) into the Response body.
            HttpResponse<InputStream> response = httpClient.send(request, streamingHandler);

            // 3. Check for bad status code immediately.
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                var errorMsg = "HTTP Error: " + response.statusCode() + " - " + request.uri();
                return new StreamResult.Failure(new IOException(errorMsg), context);
            }

            // 4. Return the successful result. The InputStream is the body() from the HttpResponse.
            return new StreamResult.Success(response.body(), context);

        } catch (IOException e) {
            // Early connection or I/O failure before body is available.
            return new StreamResult.Failure(e, context);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new StreamResult.Failure(new IOException("Request interrupted during connection.", e), context);
        }
    }

    /**
     * Shuts down the internal Virtual Thread Executor used by the HttpClient.
     * Must be called to ensure graceful termination.
     */
    @Override
    public void close() {
        if (internalExecutor != null) {
            System.out.println("\nShutting down internal Virtual Thread Executor...");
            internalExecutor.shutdownNow();
        }
    }

    // --- Extracted and Reusable Components ---

    /**
     * Provides a synchronous, pull-based stream of ByteBuffer chunks, abstracting away
     * the underlying reactive Flow API and BlockingQueue by extending InputStream.
     *
     * This version manages internal state using an Iterator to read sequential ByteBuffers from the
     * List<ByteBuffer> batch received from the network.
     */
    public static class ByteBufferChunkStream extends InputStream {
        // Queue now holds batches of ByteBuffers
        private final BlockingQueue<Optional<List<ByteBuffer>>> queue;
        private final CompletableFuture<Long> completionFuture;
        private final CompletableFuture<Flow.Subscription> subscriptionFuture;

        // State for iterating over the current List<ByteBuffer> batch
        private Iterator<ByteBuffer> currentBufferIterator = null;

        // State for the single buffer currently being read from the list
        private ByteBuffer currentBuffer = null;

        // volatile ensures visibility across threads (important for external close calls).
        private volatile boolean streamFinished = false;
        // Lock used to guard the critical section of the close() method for thread safety.
        private final ReentrantLock closeLock = new ReentrantLock();

        public ByteBufferChunkStream(
                BlockingQueue<Optional<List<ByteBuffer>>> queue,
                CompletableFuture<Long> completionFuture,
                CompletableFuture<Flow.Subscription> subscriptionFuture) {

            this.queue = queue;
            this.completionFuture = completionFuture;
            this.subscriptionFuture = subscriptionFuture;
        }

        /**
         * Internal method to get the next available ByteBuffer for reading.
         * This logic handles iterating through the current List<ByteBuffer> batch
         * before blocking to fetch a new batch from the queue.
         */
        private ByteBuffer getCurrentBuffer() throws IOException {
            // Loop until we find a readable buffer or hit end-of-stream/error
            while (true) {
                // 1. Check if the current single buffer is still readable
                if (currentBuffer != null && currentBuffer.hasRemaining()) {
                    return currentBuffer;
                }

                // 2. Try to advance to the next buffer using the iterator
                if (currentBufferIterator != null && currentBufferIterator.hasNext()) {
                    currentBuffer = currentBufferIterator.next();

                    // The buffer received from the network might have 0 remaining. Skip it.
                    if (currentBuffer.hasRemaining()) {
                        return currentBuffer;
                    }

                    // If the buffer is empty, loop to check the next one in the list (step 2 continues)
                    continue;
                }

                // 3. Current iterator exhausted (or null). Block and fetch a new list batch from the queue.
                currentBufferIterator = null; // Clear iterator state before blocking

                try {
                    // Block and take the next Optional<List<ByteBuffer>>
                    var optionalList = queue.take();

                    if (optionalList.isEmpty()) {
                        // Success completion indicator (Optional.empty() from onComplete)
                        streamFinished = true;
                        if (completionFuture.isCompletedExceptionally()) {
                            // Re-throw the exception that was set by the subscriber's onError
                            completionFuture.get();
                        }
                        return null; // End of stream
                    }

                    // Get the new list of buffers and create an iterator
                    List<ByteBuffer> newBufferList = optionalList.get();

                    // Check for ERROR_SENTINEL. The sentinel is List.of(ERROR_SENTINEL)
                    if (newBufferList.size() == 1 && newBufferList.get(0) == ERROR_SENTINEL) {
                        streamFinished = true;
                        completionFuture.get(); // Re-throw the actual exception
                        return null;
                    }

                    currentBufferIterator = newBufferList.iterator();

                    // Loop to step 2 to start processing the new list batch
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Stream consumption interrupted.", e);
                } catch (ExecutionException e) {
                    // Re-throw the actual cause of the exception
                    var cause = e.getCause() != null ? e.getCause() : e;
                    throw new IOException("Stream consumption failed: " + cause.getMessage(), cause);
                }
            }
        }

        @Override
        public int read() throws IOException {
            var b = new byte[1];
            var readCount = read(b, 0, 1);
            if (readCount == -1) {
                return -1;
            }
            return b[0] & 0xFF; // Return as an unsigned byte value (0-255)
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (streamFinished) {
                return -1;
            }
            if (len == 0) {
                return 0;
            }

            var bytesReadTotal = 0;
            var remainingToRead = len;

            while (remainingToRead > 0) {
                var currentBuffer = getCurrentBuffer();

                if (currentBuffer == null) {
                    // End of stream reached (or error occurred)
                    return bytesReadTotal > 0 ? bytesReadTotal : -1;
                }

                // Read available bytes from the current buffer
                var available = currentBuffer.remaining();
                var toRead = Math.min(available, remainingToRead);

                currentBuffer.get(b, off + bytesReadTotal, toRead);

                bytesReadTotal += toRead;
                remainingToRead -= toRead;
            }

            return bytesReadTotal;
        }

        @Override
        public void close() {
            closeLock.lock();
            try {
                if (streamFinished) {
                    return;
                }
                streamFinished = true;
            } finally {
                closeLock.unlock();
            }

            // 1. Attempt to cancel the upstream subscription (producer).
            try {
                var subscription = subscriptionFuture.get(1, java.util.concurrent.TimeUnit.MILLISECONDS);
                subscription.cancel();
            } catch (Exception e) {
                // Ignore if subscription is unavailable or fails to cancel
            }

            // 2. Clear the queue and send ERROR_SENTINEL to unblock any waiting consumer.
            queue.clear();
            try {
                // Put ERROR_SENTINEL wrapped in List.of() to match the queue type
                queue.put(Optional.of(List.of(ERROR_SENTINEL)));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }


    /**
     * Custom implementation of a BodySubscriber that bridges the reactive network stream
     * (receiving List<ByteBuffer> chunks) to a synchronous InputStream via a BlockingQueue.
     */
    public static class CustomStreamingBodySubscriber implements BodySubscriber<InputStream> {
        // Queue now holds batches of ByteBuffers
        private final BlockingQueue<Optional<List<ByteBuffer>>> queue;
        private final CompletableFuture<Long> resultFuture;
        private final CompletableFuture<Flow.Subscription> subscriptionFuture;
        private final ByteBufferChunkStream inputStream;
        // The subscription receives List<ByteBuffer> as the item type.
        private Flow.Subscription subscription;

        private final CompletableFuture<InputStream> bodyReadyFuture;

        private CustomStreamingBodySubscriber(
                BlockingQueue<Optional<List<ByteBuffer>>> queue,
                CompletableFuture<Long> resultFuture,
                CompletableFuture<Flow.Subscription> subscriptionFuture) {

            this.queue = queue;
            this.resultFuture = resultFuture;
            this.subscriptionFuture = subscriptionFuture;
            // Pass the List<ByteBuffer> queue to the stream
            this.inputStream = new ByteBufferChunkStream(queue, resultFuture, subscriptionFuture);
            this.bodyReadyFuture = CompletableFuture.completedFuture(inputStream);
        }

        /**
         * Factory method that acts as the BodyHandler implementation, setting up all
         * stream components and returning the BodySubscriber.
         *
         * @return The BodyHandler which produces an InputStream.
         */
        public static BodyHandler<InputStream> createStreamingHandler() {
            // Queue size is intentionally small to provide strong backpressure
            var queue = new LinkedBlockingQueue<Optional<List<ByteBuffer>>>(QUEUE_CAPACITY);
            var completionFuture = new CompletableFuture<Long>();
            var subscriptionFuture = new CompletableFuture<Flow.Subscription>();

            // Return the BodyHandler lambda
            return responseInfo -> new CustomStreamingBodySubscriber(queue, completionFuture, subscriptionFuture);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            this.subscriptionFuture.complete(subscription);
            // Requesting one ensures the queue doesn't overflow immediately.
            subscription.request(1);
        }

        /**
         * Receives the list of ByteBuffer chunks from the network publisher and
         * performs a single put operation to represent the entire batch.
         *
         * @param item A list of ByteBuffer chunks from the network.
         */
        @Override
        public void onNext(List<ByteBuffer> item) {
            try {
                // Put the list (the "combined" data) as a single item.
                // put() will block if the queue is full, applying backpressure.
                queue.put(Optional.of(item));

                // Request the next chunk AFTER the current list has been successfully
                // placed in the queue.
                subscription.request(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                subscription.cancel();
                onError(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            System.err.println("Stream Error in Subscriber: " + throwable.getMessage());
            // Signal the consumer thread that an error occurred
            resultFuture.completeExceptionally(throwable);
            // Put ERROR_SENTINEL (wrapped in a list) to unblock consumer.
            try {
                queue.put(Optional.of(List.of(ERROR_SENTINEL)));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void onComplete() {
            // Signal end of stream to the consumer thread.
            try {
                // Use Optional.empty() as the completion sentinel
                queue.put(Optional.empty());
                resultFuture.complete(0L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                resultFuture.completeExceptionally(e);
            }
        }

        @Override
        public CompletableFuture<InputStream> getBody() {
            // Return the future containing the InputStream (our synchronous wrapper)
            return bodyReadyFuture;
        }
    }


    /**
     * Mock class to simulate an XML Streaming parser (like StAX or SAX)
     * that consumes data synchronously from the provided InputStream.
     */
    static class XmlStreamParser implements AutoCloseable {
        private final InputStream inputStream;
        private final int requestId;

        public XmlStreamParser(InputStream is, int id) {
            this.inputStream = is;
            this.requestId = id;
        }

        /**
         * Simulates processing the XML stream, reading synchronously from the InputStream.
         * This uses the same pattern as a real StAX (Streaming API for XML) parser.
         * * @return The number of simulated XML elements processed.
         */
        public long processStream() throws IOException {
            System.out.printf("[#%d] XML Parser: Initializing StAX parser (e.g., XMLInputFactory.createXMLStreamReader(is))...%n", requestId);

            // In a real application:
            // XMLInputFactory factory = XMLInputFactory.newInstance();
            // XMLStreamReader reader = factory.createXMLStreamReader(inputStream);

            long elementsProcessed = 0;
            var buffer = new byte[1024];
            int bytesRead;
            long totalBytesConsumed = 0;
            long eventTriggerThreshold = 5120; // Simulate an event for every 5KB processed

            while ((bytesRead = inputStream.read(buffer)) != -1) {
                totalBytesConsumed += bytesRead;

                // Simulate the StAX reader advancing to the next event (reader.next())
                // NOTE: This simulated step represents the CPU-BOUND transformation work.
                if (totalBytesConsumed % eventTriggerThreshold < bytesRead) {
                    elementsProcessed++;
                    // Real logic: while(reader.hasNext()) { DataModel model = transform(reader.next()); }
                }
            }

            // Ensure we count at least one "document" if bytes were consumed
            if (totalBytesConsumed > 0 && elementsProcessed == 0) {
                elementsProcessed = 1;
            }

            System.out.printf("[#%d] XML Parser: Finished parsing stream. Total bytes consumed: %d. Total simulated StAX events processed: %d%n",
                    requestId, totalBytesConsumed, elementsProcessed);
            return elementsProcessed;
        }

        @Override
        public void close() {
            // Crucially, closing the parser should close the underlying InputStream,
            // which in turn cancels the network subscription.
            try {
                inputStream.close();
            } catch (IOException e) {
                System.err.printf("[#%d] Error closing XML stream: %s%n", requestId, e.getMessage());
            }
        }
    }


    // --- Demonstration Main Method ---
    public static void main(String[] args) {
        // Use a try-with-resources block to automatically call close() on the client
        try (StreamHttpClient client = new StreamHttpClient()) {

            // --- Concurrent Streaming XML Demo using Virtual Threads ---
            System.out.println("--- Starting Concurrent XML Stream Processing Demo using Virtual Threads ---");
            final var requestCount = 5;

            // Base URL parameters - Mocking an XML feed endpoint
            final var baseUri = "https://jsonplaceholder.typicode.com/todos/";

            var futures = new ArrayList<Future<Long>>(); // Future will hold the total elements processed

            var startTime = System.currentTimeMillis();

            // 1. Create an ExecutorService that spawns a Virtual Thread for every task.
            try (ExecutorService vtExecutor = Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("kite-vt-consumer-", 0).factory())) {

                for (var i = 0; i < requestCount; i++) {
                    final var id = i + 1;

                    // Intentionally use a non-existent ID for the 4th request (i=3)
                    final var requestUri = URI.create(baseUri + (i == 3 ? "1000" : id));

                    var request = HttpRequest.newBuilder(requestUri)
                            // Requesting XML data
                            .header("Accept", "application/xml, text/xml")
                            .GET()
                            .build();

                    // Create and populate a Context object for each request
                    final var requestContext = new Context()
                            .put("request_id", UUID.randomUUID().toString())
                            .put("request_index", id);

                    System.out.printf("Submitting XML streaming request #%d for %s (Context ID: %s)...%n",
                            id, request.uri(), requestContext.get("request_id").orElse("N/A"));

                    // 2. Submit the synchronous consumption logic to a Virtual Thread.
                    // THIS VIRTUAL THREAD WILL HANDLE BOTH THE BLOCKING I/O READS AND THE
                    // CPU-BOUND XML PARSING/TRANSFORMATION, isolating this work from
                    // the main server I/O threads.
                    var future = vtExecutor.submit(() -> {
                        var elementsProcessed = 0L;

                        // Call stream with HttpRequest
                        var result = client.stream(request, requestContext);

                        // Use switch expression for clean pattern matching
                        return switch (result) {
                            case StreamResult.Success successResult -> {
                                // Successful connection and 2xx status, proceed with streaming

                                // Pass the InputStream directly to the streaming XML parser
                                try (var xmlParser = new XmlStreamParser(successResult.inputStream(), id)) {

                                    // Access and display context metadata
                                    var contextId = successResult.context().get("request_id").map(Object::toString).orElse("N/A");
                                    System.out.printf("[#%d] Consumer VT started XML parsing on thread: %s (Context ID: %s)%n",
                                            id, Thread.currentThread().getName(), contextId);

                                    // The parser pulls data from the InputStream synchronously
                                    elementsProcessed = xmlParser.processStream();

                                    // Stream finished successfully
                                    System.out.printf("[#%d] XML Stream finished. Processed %d simulated StAX events.%n", id, elementsProcessed);
                                    yield elementsProcessed;
                                } catch (IOException e) {
                                    System.err.printf("[#%d] I/O Exception during XML parsing: %s%n", id, e.getMessage());
                                    // Throw RuntimeException to propagate the error out of the Future
                                    throw new RuntimeException(e);
                                }
                            } case StreamResult.Failure failureResult -> {
                                // Early connection or status error handled here
                                var contextId = failureResult.context().get("request_id").map(Object::toString).orElse("N/A");
                                System.err.printf("[#%d] Early Request Failure (Context ID: %s): %s%n",
                                        id, contextId, failureResult.error().getMessage());
                                // Throw RuntimeException to propagate the error out of the Future
                                throw new RuntimeException(failureResult.error());
                            }
                        };
                    });
                    futures.add(future);
                }

                // 3. Wait for all tasks (Futures) to complete and retrieve results.
                var totalElementsProcessed = 0L;
                for (var i = 0; i < requestCount; i++) {
                    try {
                        var elements = futures.get(i).get(); // .get() blocks until the VT finishes
                        System.out.printf("--- XML Stream Result #%d: Processed %d simulated StAX events.%n", i + 1, elements);
                        totalElementsProcessed += elements;
                    } catch (ExecutionException e) {
                        System.err.printf("--- XML Stream Request #%d FAILED: %s%n", i + 1, e.getCause().getMessage());
                    } catch (Exception e) {
                        System.err.printf("--- XML Stream Request #%d FAILED: %s%n", i + 1, e.getMessage());
                    }
                }

                System.out.printf("\nTotal simulated StAX events processed (excluding failed): %d%n", totalElementsProcessed);

            } // vtExecutor is automatically shut down here by AutoCloseable

            var endTime = System.currentTimeMillis();
            System.out.printf("Total Concurrent Execution Time (on VTs): %d ms%n", endTime - startTime);

        } catch (Exception e) {
            System.err.println("An unexpected error occurred in the main process: " + e.getMessage());
        }
    }
}