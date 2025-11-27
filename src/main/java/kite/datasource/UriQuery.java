package kite.datasource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the immutable, UNENCODED query component of a URI.
 * It stores the raw parameters, deferring string assembly and encoding to the builder.
 */
public record UriQuery(Map<String, List<String>> parameters) {

    /**
     * Compact constructor (runs before fields are implicitly assigned).
     * This enforces validation on the map structure, ensuring all keys are valid
     * and all value lists are cleaned, converting null values to empty strings.
     */
    public UriQuery {
        var cleanedParameters = new LinkedHashMap<String, List<String>>();

        for (var entry : parameters.entrySet()) {
            var key = entry.getKey();
            var rawValues = entry.getValue();

            // 1. Skip keys that are null or blank (query parameters must have a name)
            if (key == null || key.isBlank()) continue;

            // 2. Convert null values to empty strings. Empty strings are valid parameter values (e.g., ?key=).
            var cleanedValues = rawValues.stream()
                    .map(v -> v == null ? "" : v)
                    .toList();

            // 3. Only add the entry if there are values left (even if they are all empty strings).
            if (!cleanedValues.isEmpty()) {
                cleanedParameters.put(key, cleanedValues);
            }
        }
        // Assign the cleaned, immutable map back to the record field.
        parameters = Map.copyOf(cleanedParameters);
    }

    /**
     * Factory method to create a UriQuery from an array of key-value segments.
     * Arguments must be provided in pairs: key1, value1, key2, value2, ...
     * * <p>Key characteristics:</p>
     * <ul>
     * <li>**Normalization:** If the input array is unbalanced (odd number of arguments), the last key is
     * automatically assigned an empty string (`""`) value.</li>
     * <li>**Multi-Value Keys:** Duplicate keys in the input are supported and collected into a list of values.</li>
     * <li>**Validation:** Invalid keys (null or blank) are ignored. Null values are converted to empty strings (`""`).</li>
     * </ul>
     * * <p>Examples:</p>
     * <pre>
     * // Standard case
     * UriQuery.of("sort", "date", "limit", "10");
     * // -> parameters: {"sort": ["date"], "limit": ["10"]}
     * * // Multi-value and normalization (key2 is unbalanced)
     * UriQuery.of("id", "100", "id", "200", "key2");
     * // -> parameters: {"id": ["100", "200"], "key2": [""]}
     * * // Validation (null key and null value converted)
     * UriQuery.of(null, "ignore this", "key", null);
     * // -> parameters: {"key": [""]}
     * </pre>
     * * @param keyValues The alternating key and value components.
     * @return The UriQuery component (always succeeds after normalization and filtering).
     */
    public static UriQuery of(String... keyValues) {

        // Normalize input: if unbalanced, assume the last key has an empty value ("")
        var normalizedKeyValues = new ArrayList<>(Arrays.asList(keyValues));

        if (normalizedKeyValues.size() % 2 != 0) {
            // Add an empty string as the value for the dangling key
            normalizedKeyValues.add("");
        }
        // The list is now guaranteed to be balanced (even size).

        var params = new LinkedHashMap<String, List<String>>();

        for (int i = 0; i < normalizedKeyValues.size(); i += 2) {
            var key = normalizedKeyValues.get(i);
            var value = normalizedKeyValues.get(i + 1);

            // Note: The key validation is performed by the compact constructor.

            params.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        }

        // The compact constructor handles the final filtering and immutability check
        return new UriQuery(params);
    }

    /**
     * Factory method to create an empty query component.
     * @return A UriQuery with an empty map of parameters.
     */
    public static UriQuery empty() {
        return new UriQuery(Map.of());
    }
}