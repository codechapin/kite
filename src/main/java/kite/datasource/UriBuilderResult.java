package kite.datasource;

import java.net.URI;

/**
 * Sealed interface to represent the result of the URI building process.
 */
public sealed interface UriBuilderResult permits UriBuilderResult.Success, UriBuilderResult.Failure {
    /** Represents a successfully constructed URI. */
    record Success(URI uri) implements UriBuilderResult {}
    /** Represents a failure during construction, containing the reason. */
    record Failure(String reason) implements UriBuilderResult {}
}
