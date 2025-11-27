package kite.datasource;

import java.io.InputStream;

public record Response(int status, InputStream body) {
    // should be sealed for pattern matching: ok or error?
}
