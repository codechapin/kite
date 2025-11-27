package kite.io;

import java.nio.ByteBuffer;
import java.util.List;

public class KiteIO {
    public static int remaining(List<ByteBuffer> buffers, int max) {
        if (buffers == null) return 0;

        int remain = 0;
        for (var buffer : buffers) {
            remain += buffer.remaining();
            if (remain > max) {
                throw new IllegalArgumentException("too many bytes");
            }
        }

        return remain;
    }
}
