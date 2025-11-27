package kite.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Custom InputStream implementation that pulls data from an internal BlockingQueue.
 * This acts as the consumer/reader side of the pipe.
 */
public class VirtualPipedInputStream extends InputStream {

    // A special marker to signal an error has occurred when reading from the queue.
    static final ByteBuffer ERROR_SENTINEL = ByteBuffer.wrap(new byte[0]).asReadOnlyBuffer();

    /** The default maximum number of ByteBuffer chunks allowed in the internal buffer. */
    public static final int DEFAULT_QUEUE_SIZE = 10;

    // The internal, hidden BlockingQueue for buffer chunks
    private final BlockingQueue<Optional<ByteBuffer>> queue;

    // Reference to the connected output stream
    private VirtualPipedOutputStream source = null;

    // Current buffer being read from.
    private ByteBuffer currentBuffer;

    // Stores an error if the producer signals one.
    private volatile IOException error = null;

    // Reference to the producer Virtual Thread to interrupt it on close.
    private Thread producerVt;

    /**
     * Creates a VirtualPipedInputStream with a specified queue size.
     * @param queueSize The maximum number of ByteBuffer chunks allowed in the internal buffer.
     */
    public VirtualPipedInputStream(int queueSize) {
        this.queue = new LinkedBlockingQueue<>(queueSize);
    }

    /**
     * Creates a VirtualPipedInputStream with the default queue size (10).
     */
    public VirtualPipedInputStream() {
        this(DEFAULT_QUEUE_SIZE); // Default queue size of 10 chunks
    }

    /**
     * Establishes a connection between this input stream and the specified output stream.
     * @param src The VirtualPipedOutputStream to connect to.
     * @throws IOException if already connected.
     */
    public void connect(VirtualPipedOutputStream src) throws IOException {
        if (this.source != null) {
            throw new IOException("Pipe already connected.");
        }

        this.source = src;
        src.connect(this); // Establish back-reference
    }

    /**
     * Sets the producer thread that needs to be interrupted if the consumer closes early.
     */
    public void setProducerThread(Thread vt) {
        this.producerVt = vt;
    }

    /**
     * Sets the error state, typically called by the VirtualPipedOutputStream.
     */
    public void setError(IOException e) {
        this.error = e;
    }


    /**
     * Checks for any error signaled by the producer and throws it.
     */
    private void checkForError() throws IOException {
        if (error != null) {
            throw error;
        }
    }

    @Override
    public int read() throws IOException {
        checkForError(); // Check for pre-existing errors
        var singleByte = new byte[1];
        int bytesRead = read(singleByte, 0, 1);
        if (bytesRead == -1) {
            return -1;
        }
        return singleByte[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }

        checkForError();

        // 1. Drain the current buffer if it has remaining data
        if (currentBuffer != null && currentBuffer.hasRemaining()) {
            int bytesToRead = Math.min(len, currentBuffer.remaining());
            currentBuffer.get(b, off, bytesToRead);
            return bytesToRead;
        }

        // 2. The current buffer is empty/null, so pull the next chunk from the queue.
        try {
            // This blocks the consumer VT on the queue (fast, in-memory)
            Optional<ByteBuffer> nextChunk = queue.take();

            if (nextChunk.isEmpty()) {
                // End of Stream signal from the producer
                currentBuffer = null;
                return -1;
            }

            currentBuffer = nextChunk.get();

            // Check for ERROR_SENTINEL (used by producer on failure or early close)
            if (currentBuffer == ERROR_SENTINEL) {
                currentBuffer = null; // Clear buffer
                checkForError(); // This throws the actual error
                return -1; // Should not be reached, but as fallback
            }

            // 3. Read from the newly fetched buffer
            int bytesToRead = Math.min(len, currentBuffer.remaining());
            currentBuffer.get(b, off, bytesToRead);
            return bytesToRead;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Stream read operation interrupted.", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (producerVt != null && producerVt.isAlive()) {
            // If the consumer closes the stream early, interrupt the producer VT
            System.out.printf("[VT Consumer %s] Closing stream early, interrupting producer VT: %s%n",
                    Thread.currentThread().getName(), producerVt.getName());
            producerVt.interrupt();
        }

        // Clear internal state
        queue.clear();
        currentBuffer = null;

        // Clean up connection references
        if (source != null) {
            source.error("Pipe closed by reader.");
        }

        super.close();
    }

    void putInQueue(Optional<ByteBuffer> buffer, String errorMsg) throws IOException {
        try {
            queue.put(buffer);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(errorMsg, e);
        }
    }
}

