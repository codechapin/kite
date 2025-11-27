package kite.datasource;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Arrays;

// Import all required components from the new package


/**
 * Utility class for creating URIs in a clean, programmatic way using the Fluent Builder pattern.
 * This class demonstrates the usage of the modular UriBuilder components, which now
 * strictly require pre-built UriPath and UriQuery objects.
 */
public class UriBuilderDemo {

    /**
     * Helper method to process the result, outputting success or failure messages.
     * @param result The result object from the UriBuilder.build() call.
     * @param exampleName A descriptive name for the example.
     */
    private static void processResult(UriBuilderResult result, String exampleName) {
        switch (result) {
            case UriBuilderResult.Success success -> System.out.printf("Built URI (%s): %s%n", exampleName, success.uri());
            case UriBuilderResult.Failure failure -> System.err.printf("Failed to build URI (%s): %s%n", exampleName, failure.reason());
        }
    }

    /**
     * Main method to demonstrate the usage of the new Fluent API (UriBuilder).
     */
    public static void main(String[] args) {
        System.out.println("--- Kite URI Builder Demonstration (Component-Focused API) ---");
        System.out.println("--- Builder now requires explicit Path/Query components. ---");


        // Example 1: Full URI construction (Success case) - Tests path/query encoding
        // Components must now be created before being passed to the builder.
        // Expected URI: https://secure.api.com:8443/v1/users/search%20query?sort%20by=date&limit=10%20items
        var path1 = UriPath.of("v1", "users", "search query"); // Component handles raw input
        var query1 = UriQuery.of("sort by", "date", "limit", "10 items"); // Component handles raw input

        var result1 = UriBuilder.from("https", "secure.api.com", 8443)
                .path(path1)
                .query(query1)
                .build();

        processResult(result1, "1. Fluent Full Build (Success - Encoding Test)");

        // Example 2: Minimal construction with default HTTPS
        var path2 = UriPath.of("report", "monthly data");
        var query2 = UriQuery.of("name", "John Smith", "location", "New York");

        var result2 = UriBuilder.from("analytics.com")
                .path(path2)
                .query(query2)
                .build();

        processResult(result2, "2. Fluent Minimal Build (Success)");

        // Example 3 (Was 4): Build failure due to missing host
        System.out.println("\n--- Testing Final Build Failure (Required Host) ---");

        var failPath3 = UriPath.of("api", "data");

        var buildFailResult = UriBuilder.from(null) // Pass null host
                .path(failPath3)
                .build();

        processResult(buildFailResult, "3. Fluent Build Failure (Missing Host)");

        // Example 4 (Was 5): Clean success case - Uses from(host, port)
        System.out.println("\n--- Clean Success Case ---");

        var path4 = UriPath.of("checkout", "process");
        var query4 = UriQuery.of("session_id", "xyz123", "coupon", "SAVE10");

        var cleanSuccessResult = UriBuilder.from("store.site.org", 443)
                .path(path4)
                .query(query4)
                .build();

        processResult(cleanSuccessResult, "4. Clean Fluent Success");

        // Example 5 (Was 6): Demonstrating Component Usage
        System.out.println("\n--- Standard Component Usage (Required Approach) ---");

        // Pre-create components
        var prePath = UriPath.of("pre validated path with spaces");
        var preQuery = UriQuery.of("source", "external api", "cache", "true");

        var overloadedResult = UriBuilder.from("prebuilt.example.com")
                .path(prePath)
                .query(preQuery)
                .build();

        processResult(overloadedResult, "5. Component Usage");

        // Example 6 (Was 8): Testing explicit path component construction
        // This relies on UriPath's compact constructor validating/cleaning invalid segments.
        System.out.println("\n--- Testing Explicit Path Component Construction ---");
        List<String> invalidSegments = Arrays.asList("safe", null, "", "path", "segment");

        UriPath explicitlyBuiltPath = new UriPath(invalidSegments);

        var explicitPathResult = UriBuilder.from("hardened.validation.com")
                .path(explicitlyBuiltPath)
                .build();

        // Expected URI: https://hardened.validation.com/safe/path/segment
        processResult(explicitPathResult, "6. Explicit Path Component");

        // Example 7 (Was 9): Testing explicit query component construction
        // This relies on UriQuery's compact constructor validating/cleaning invalid keys and values.
        System.out.println("\n--- Testing Explicit Query Component Construction ---");

        Map<String, List<String>> invalidParams = new LinkedHashMap<>();
        // The null value here will be converted to an empty string ("") by UriQuery's internal logic
        invalidParams.put("valid_key", Arrays.asList("value1", null, "value2"));
        invalidParams.put(" ", Arrays.asList("should be ignored"));
        invalidParams.put(null, Arrays.asList("also ignored"));
        invalidParams.put("empty_list", Arrays.asList());

        UriQuery explicitlyBuiltQuery = new UriQuery(invalidParams);

        var explicitQueryResult = UriBuilder.from("query.validation.com")
                .query(explicitlyBuiltQuery)
                .build();

        // Expected URI: https://query.validation.com/?valid_key=value1&valid_key=&valid_key=value2
        processResult(explicitQueryResult, "7. Explicit Query Component");
    }
}