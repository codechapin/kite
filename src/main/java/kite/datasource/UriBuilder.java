package kite.datasource;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Collectors;



/**
 * The primary public record for building URIs fluently.
 * It tracks the component state and executes the final assembly/encoding logic.
 * * NOTE: The builder is now component-focused and only accepts UriPath and UriQuery objects,
 * requiring that raw string input and merging be handled by those component classes before they
 * are passed to the builder.
 */
public record UriBuilder(
        String scheme,
        String host,
        int port, // Changed from Optional<Integer> to raw int. -1 is the default sentinel value.
        UriPath path,
        UriQuery query
) {

    /**
     * Compact constructor (runs before fields are implicitly assigned).
     * This enforces initialization logic, guaranteeing internal component integrity
     * even if the public canonical constructor is called directly.
     */
    public UriBuilder {
        // port is int and defaults to -1 via factory methods/resolve helper, so no null check needed.
        path = path == null ? UriPath.empty() : path;
        query = query == null ? UriQuery.empty() : query;
    }

    // --- Helper for Port Resolution ---

    /**
     * Helper to resolve an integer port value.
     * Ports <= 0 are resolved to the sentinel value -1, indicating no explicit port
     * should be included in the final URI.
     */
    private static int resolvePortInt(int port) {
        // If port is > 0, use it. If <= 0, use the sentinel value -1.
        return port > 0 ? port : -1;
    }

    // --- Static Factory Methods for Initial Configuration (Authority) ---

    /** Creates a URI builder starting with the host (scheme defaults to https). */
    public static UriBuilder from(String host) {
        // Pass the default sentinel value -1 for port.
        return new UriBuilder(null, host, -1, null, null);
    }

    /** Creates a URI builder starting with the host and port (scheme defaults to https). */
    public static UriBuilder from(String host, int port) {
        // Use resolvePortInt to handle validation and set -1 if port <= 0.
        return new UriBuilder(null, host, resolvePortInt(port), null, null);
    }

    /** Creates a URI builder starting with the scheme and host. */
    public static UriBuilder from(String scheme, String host) {
        // Pass the default sentinel value -1 for port.
        return new UriBuilder(scheme, host, -1, null, null);
    }

    /** Creates a URI builder starting with the scheme, host, and port. */
    public static UriBuilder from(String scheme, String host, int port) {
        // Use resolvePortInt to handle validation and set -1 if port <= 0.
        return new UriBuilder(scheme, host, resolvePortInt(port), null, null);
    }

    // --- Chaining Methods ---

    /** * Sets the path using a pre-built UriPath component.
     * All path segment validation and raw input parsing must be handled when
     * the UriPath object is created.
     */
    public UriBuilder path(UriPath path) {
        return new UriBuilder(scheme, host, port, path, query);
    }

    /** * Replaces the query using a pre-built UriQuery component.
     * All query parameter validation and raw input parsing must be handled when
     * the UriQuery object is created. This method is destructive to the existing query.
     */
    public UriBuilder query(UriQuery query) {
        return new UriBuilder(scheme, host, port, path, query);
    }

    // The mergeQuery(String...) and mergeQuery(UriQuery) methods were removed
    // to simplify the builder. Merging logic should be handled by the UriQuery component itself
    // before it is passed to this builder's query() method.


    /**
     * Executes the creation process, performing final string assembly and URL encoding.
     * <p>Examples:</p>
     * <pre>
     * // Success Case (with encoding)
     * UriBuilder.from("api.example.com", 8080)
     * .path(UriPath.of("v1", "search data")) // Component must be created separately
     * .query(UriQuery.of("q", "hello world")) // Component must be created separately
     * .build();
     * // -> UriBuilderResult.Success(URI: https://api.example.com:8080/v1/search%20data?q=hello%20world)
     *
     * // Failure Case (missing host)
     * UriBuilder.from(null)
     * .build();
     * // -> UriBuilderResult.Failure("URI host must be set.")
     * * // Invalid Port Case (now succeeds without port number)
     * UriBuilder.from("host.com", 0)
     * .build();
     * // -> UriBuilderResult.Success(URI: https://host.com/)
     * </pre>
     * @return A {@link UriBuilderResult} (Success or Failure).
     */
    public UriBuilderResult build() {

        var effectiveScheme = (scheme == null || scheme.isBlank()) ? "https" : scheme;

        // Host validation
        if (host == null || host.isBlank()) {
            return new UriBuilderResult.Failure("URI host must be set.");
        }

        // 1. Get raw components and join them into strings.

        // Path assembly: Join segments with '/'
        var rawPath = path.segments().stream()
                .collect(Collectors.joining("/"));

        // Query assembly: Join key=value pairs with '&'
        var rawQuery = query.parameters().entrySet().stream()
                // FlatMap to handle the List<String> values for multi-value keys
                .flatMap(entry -> {
                    // Define key locally to ensure unambiguous closure access in the inner stream (for IDEs)
                    final String key = entry.getKey();

                    return entry.getValue().stream()
                            .map(queryValue -> { // Renamed 'value' to 'queryValue'
                                String effectiveValue = (queryValue == null) ? "" : queryValue;
                                // Combine raw key (local variable) and raw value (lambda param) into the format 'key=value'
                                return key + "=" + effectiveValue;
                            });
                })
                // Join all raw key=value pairs with '&'
                .collect(Collectors.joining("&"));


        // 2. Construct Authority part
        var authority = host;
        // Only include the port if it is greater than 0 (i.e., not the sentinel value -1 or 0)
        if (port > 0) {
            authority += ":" + port;
        }

        // 3. Construct Path part: Must start with a slash and is the raw path.
        var finalPathPart = rawPath.isEmpty() ? "/" : "/" + rawPath;

        // 4. Construct Query part: Null if empty, otherwise the raw query string.
        var finalQueryPart = rawQuery.isEmpty() ? null : rawQuery;

        try {
            // Use the 5-argument URI constructor, which performs the necessary encoding
            var uri = new URI(
                    effectiveScheme,
                    authority,
                    finalPathPart,
                    finalQueryPart,
                    null // Fragment (hash) part is not supported by the current API
            );

            return new UriBuilderResult.Success(uri);
        } catch (URISyntaxException e) {
            return new UriBuilderResult.Failure("Malformed URI generated: " + e.getMessage());
        }
    }
}