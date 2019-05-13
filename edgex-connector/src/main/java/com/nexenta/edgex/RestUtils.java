package com.nexenta.edgex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Utilities useful for REST/HTTP S3Service implementations.
 */
public class RestUtils {

    /**
     * The set of request parameters which must be included in the canonical
     * string to sign.
     */
    private static final List<String> SIGNED_PARAMETERS = Arrays.asList(new String[] {
            "acl", "torrent", "logging", "location", "policy", "requestPayment", "versioning",
            "versions", "versionId", "notification", "uploadId", "uploads", "partNumber", "website",
            "delete", "lifecycle", "tagging", "cors", "restore", "replication", "accelerate",
            "inventory", "analytics", "metrics",
            "response-cache-control",
            "response-content-disposition",
            "response-content-encoding",
            "response-content-language",
            "response-content-type",
            "response-expires"
    });

    /**
     * Calculate the canonical string for a REST/HTTP request to S3.
     *
     * @param method
     *            The HTTP verb.
     * @param resource
     *            The HTTP-encoded resource path.
     * @param request
     *            The request to be canonicalized.
     * @param expires
     *            When expires is non-null, it will be used instead of the Date
     *            header.
     * @return The canonical string representation for the given S3 request.
     */
    public static String makeS3CanonicalString(String method,
            String resource,
            Map<String, String> headersMap,
            Map<String, List<String>> requestParameters,
            String expires) {

        StringBuilder buf = new StringBuilder();
        buf.append(method + "\n");

        // Add all interesting headers to a list, then sort them.  "Interesting"
        // is defined as Content-MD5, Content-Type, Date, and x-amz-
        SortedMap<String, String> interestingHeaders = new TreeMap<String, String>();
        if (headersMap != null && headersMap.size() > 0) {
            Iterator<Map.Entry<String, String>> headerIter = headersMap.entrySet().iterator();
            while (headerIter.hasNext()) {
                Map.Entry<String, String> entry = (Map.Entry<String, String>) headerIter.next();
                String key = entry.getKey();
                String value = entry.getValue();

                if (key == null) continue;
                String lk = key.toLowerCase();

                // Ignore any headers that are not particularly interesting.
                if (lk.equals("content-type") || lk.equals("content-md5") || lk.equals("date") ||
                    lk.startsWith("x-amz-"))
                {
                    interestingHeaders.put(lk, value);
                }
            }
        }

        // Remove default date timestamp if "x-amz-date" is set.
        if (interestingHeaders.containsKey("x-amz-date")) {
            interestingHeaders.put("date", "");
        }

        // Use the expires value as the timestamp if it is available. This trumps both the default
        // "date" timestamp, and the "x-amz-date" header.
        if (expires != null) {
            interestingHeaders.put("date", expires);
        }

        // These headers require that we still put a new line in after them,
        // even if they don't exist.
        if (! interestingHeaders.containsKey("content-type")) {
            interestingHeaders.put("content-type", "");
        }
        if (! interestingHeaders.containsKey("content-md5")) {
            interestingHeaders.put("content-md5", "");
        }

        // Any parameters that are prefixed with "x-amz-" need to be included
        // in the headers section of the canonical string to sign
        for (Map.Entry<String, List<String>> parameter : requestParameters
                .entrySet()) {
            if (parameter.getKey().startsWith("x-amz-")) {
                StringBuilder parameterValueBuilder = new StringBuilder();
                /**
                 *
                 * We don't need to url encode here. If a parameter has multiple
                 * values, then those values needs to be combined to a comma
                 * separated list and assigned to the header.
                 *
                 * Reference : http://docs.aws.amazon.com/AmazonS3/latest/dev/
                 * RESTAuthentication
                 * .html#RESTAuthenticationRequestCanonicalization
                 */
                for (String value : parameter.getValue()) {
                    if (parameterValueBuilder.length() > 0) {
                        parameterValueBuilder.append(",");
                    }
                    parameterValueBuilder.append(value);
                }
                interestingHeaders.put(parameter.getKey(),
                        parameterValueBuilder.toString());
            }
        }

        // Add all the interesting headers (i.e.: all that startwith x-amz- ;-))
        for (Iterator<Map.Entry<String, String>> i = interestingHeaders.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<String, String> entry = (Map.Entry<String, String>) i.next();
            String key = (String) entry.getKey();
            String value = entry.getValue();

            if (key.startsWith("x-amz-")) {
                buf.append(key).append(':');
                if (value != null) {
                    buf.append(value);
                }
            } else if (value != null) {
                buf.append(value);
            }
            buf.append("\n");
        }

        // Add all the interesting parameters
        buf.append(resource);
        String[] parameterNames = requestParameters.keySet().toArray(
                                new String[requestParameters.size()]);
        Arrays.sort(parameterNames);

        StringBuilder queryParams = new StringBuilder();
        for (String parameterName : parameterNames) {
            if ( !SIGNED_PARAMETERS.contains(parameterName) ) {
                continue;
            }

            /**
             * As per the spec given in the below URL, it is not clear as to
             * whether we need to sort the parameter values when forming the
             * string to sign. This is something that needs to be watched if we
             * receive signing problems.
             *
             * Reference : http://docs.aws.amazon.com/AmazonS3/latest/dev/
             * RESTAuthentication
             * .html#RESTAuthenticationRequestCanonicalization
             */
            List<String> values = requestParameters.get(parameterName);
            for (String value : values) {
                queryParams = queryParams.length() > 0 ? queryParams
                        .append("&") : queryParams.append("?");

                queryParams.append(parameterName);
                if (value != null) {
                    queryParams.append("=").append(value);
                }
            }
        }
        buf.append(queryParams.toString());

        return buf.toString();
    }

}
