package com.twitter.kinesis.connection;

import java.net.URI;

public class UriStrategy {
    public static final String BASE_GNIP_STREAM_URI = "https://stream.gnip.com:443/accounts/%s/publishers/%s/streams/%s/%s.json";

    public URI createStreamUri(String account, String publisher,  String product, String streamLabel, String clientId) {
        if (account == null || account.trim().isEmpty()) {
            throw new IllegalArgumentException("The account cannot be null or empty");
        }
        if (publisher == null || publisher.trim().isEmpty()) {
            throw new IllegalArgumentException("The publisher cannot be null or empty");
        }
        if (product == null || product.trim().isEmpty()) {
            throw new IllegalArgumentException("The product cannot be null or empty");
        }
        if (streamLabel == null || streamLabel.trim().isEmpty()) {
            throw new IllegalArgumentException("The streamLabel cannot be null or empty");
        }

        String uriString = String.format(BASE_GNIP_STREAM_URI, account.trim(), publisher.trim(), product.trim(), streamLabel.trim());
        if (clientId != null && !clientId.isEmpty() ){
            uriString = uriString + "?client=" + clientId;
        }

        return URI.create(uriString);
    }

}
