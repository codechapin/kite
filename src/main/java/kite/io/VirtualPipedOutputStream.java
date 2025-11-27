package kite.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import static kite.io.VirtualPipedInputStream.ERROR_SENTINEL;

/**
 * Custom OutputStream implementation that writes data into a connected InputStream's internal BlockingQueue.
 * This acts as the producer/writer side of the pipe.
 */
public class VirtualPipedOutputStream extends OutputStream {

    // Reference to the connected InputStream (the data sink)
    private VirtualPipedInputStream sink = null;

    // Stores an error if the consumer signals one. Used for local checks.
    private volatile IOException error = null;


    /**
     * Establishes a connection between this output stream and the specified input stream.
     * This method is typically called by the VirtualPipedInputStream's connect method.
     * @throws IOException if this stream is already connected.
     */
    void connect(VirtualPipedInputStream snk) throws IOException {
        if (this.sink != null) {
            throw new IOException("Already connected.");
        }

        this.sink = Objects.requireNonNull(snk);
    }

    /**
     * Ensures the pipe is connected before proceeding.
     * @throws IOException if the pipe is not connected.
     */
    private void ensureConnected() throws IOException {
        if (sink == null) {
            throw new IOException("Pipe not connected to input stream.");
        }
    }

    /**
     * Checks for any error signaled by the consumer and throws it.
     */
    private void checkForError() throws IOException {
        if (error != null) {
            throw error;
        }
    }

    /**
     * Signals an error to the connected input stream.
     */
    public void signalError(IOException e) {
        this.error = e;
        if (sink != null) {
            sink.setError(e);
        }
        // Push the sentinel to unblock any waiting producer threads
        try {
            // If connected, push the sentinel to the input stream's queue
            if (sink != null) {
                sink.putInQueue(Optional.of(ERROR_SENTINEL), "");
            }
        } catch (IOException ignored) {
            Thread.currentThread().interrupt();
        }
    }


    @Override
    public void write(int b) throws IOException {
        checkForError();
        write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) return;

        ensureConnected();
        checkForError();

        // Wrap the byte array segment into a ByteBuffer
        var buffer = ByteBuffer.wrap(b, off, len);
        // This blocks the producer VT if the queue is full (backpressure)
        sink.putInQueue(Optional.empty(), "Pipe write operation interrupted.");
    }

    @Override
    public void close() throws IOException {
        // Signal End-of-Stream (EOS) to the consumer by pushing an empty Optional
        if (error == null && sink != null) {
            sink.putInQueue(Optional.empty(), "Pipe close operation interrupted.");
        }
        super.close();
    }

    void error(String msg) {
        this.sink = null;
        this.error = new IOException(msg);
    }
}
