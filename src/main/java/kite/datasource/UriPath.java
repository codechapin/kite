package kite.datasource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Represents the immutable, UNENCODED path component of a URI.
 * It stores the raw list of segments, deferring string assembly and encoding to the builder.
 */
public record UriPath(List<String> segments) {

    /**
     * Compact constructor (runs before fields are implicitly assigned).
     * This enforces segment validation, ensuring that the segments list is always
     * filtered and clean, even if the public canonical constructor is called directly.
     */
    public UriPath {
        // Re-filter the input list to remove null or blank segments
        // This modified 'segments' list is what will be assigned to the record field.
        segments = segments.stream()
                .filter(s -> s != null && !s.isBlank())
                .toList();
    }

    /**
     * Factory method to create an UNENCODED UriPath from an array of segments (varargs).
     * <p>Examples:</p>
     * <pre>
     * // Standard case
     * UriPath.of("v1", "users", "123");
     * // -> segments: ["v1", "users", "123"]
     *
     * // Filtering: Ignores null and blank segments
     * UriPath.of("safe", null, "", "path", "segment");
     * // -> segments: ["safe", "path", "segment"]
     * </pre>
     * * @param segments The path components (e.g., "v1", "users", "search data").
     * @return The UriPath component (always succeeds after filtering).
     */
    public static UriPath of(String... segments) {
        // Delegates to the stream method for centralized construction
        return of(Arrays.stream(segments));
    }

    /**
     * Factory method to create an UNENCODED UriPath from a list of segments.
     * @param segments The path components as a List.
     * @return The UriPath component (always succeeds after filtering).
     */
    public static UriPath of(List<String> segments) {
        // Delegates to the stream method for centralized construction
        return of(segments.stream());
    }

    /**
     * Factory method to create an UNENCODED UriPath from a stream of segments.
     * This method now primarily performs the conversion to a List which is then validated
     * by the compact constructor.
     * @param segments The stream of path components.
     * @return The UriPath component (always succeeds after filtering).
     */
    public static UriPath of(Stream<String> segments) {
        // Collect into a list and let the compact constructor handle the filtering
        var rawSegments = segments.toList();

        return new UriPath(rawSegments);
    }

    /**
     * Factory method to create an empty path component.
     * @return A UriPath with an empty list of segments.
     */
    public static UriPath empty() {
        return new UriPath(List.of());
    }
}