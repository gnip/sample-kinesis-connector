package com.twitter.kinesis.connection;

import com.twitter.kinesis.utils.Environment;
import com.google.common.base.Charsets;
import com.google.inject.Inject;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class GnipHttpClient {
    private final Logger logger = Logger.getLogger(getClass());
    private Environment environment;
    private UriStrategy uriStrategy;

    @Inject
    public GnipHttpClient(Environment environment,
                          UriStrategy uriStrategy) {
        this.environment = environment;
        this.uriStrategy = uriStrategy;
    }

    public InputStream getStreaming() throws IOException, GnipConnectionException {
        URI streamUri;
        try {
            streamUri = uriStrategy.createStreamUri(
                    environment.accountName(),
                    environment.publisher(),
                    environment.product(),
                    environment.streamLabel(),
                    environment.clientId());
        } catch (IllegalArgumentException e) {
            logger.error("Unable to construct URI for Gnip API", e);
            throw e;
        }

        logger.info("Making streaming connection to " + streamUri.toString());

        HttpURLConnection connection = getConnection(new URL(streamUri.toString()), "GET", true);
        int responseCode = connection.getResponseCode();
        if (responseCode < 200 || responseCode >= 300) {
            handleNonSuccessResponse(connection);
        }

        logger.info("Succesfully made connection to " + streamUri.toString());
        return connection.getInputStream();
    }

    private HttpURLConnection getConnection(URL url, String method, boolean output) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setReadTimeout(1000 * 18); // 18 seconds covers the 15 second keep-alive
        connection.setConnectTimeout(1000 * 10);
        connection.setRequestMethod(method);
        connection.setDoOutput(output);

        connection.setRequestProperty("Authorization",
                createAuthHeader(environment.userName(),
                        environment.userPassword()));

        connection.setRequestProperty("Accept-Encoding", "gzip");

        return connection;
    }

    private String createAuthHeader(String username, String password) {
        String authToken = username + ":" + password;
        byte[] authTokenBytes = authToken.getBytes(Charsets.UTF_8);
        return "Basic " + new String(Base64.encodeBase64(authTokenBytes), Charsets.UTF_8);
    }

    private void handleNonSuccessResponse(HttpURLConnection connection) throws IOException, GnipConnectionException {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        new GZIPInputStream(connection.getErrorStream()), StandardCharsets.UTF_8));
        String error = reader.readLine();
        if (error == null) {
            error = "No message";
        }
        reader.close();

        String message = String.format("Error making %s request to %s Response code: %d, Reason: %s, Message: %s",
                connection.getRequestMethod(),
                connection.getURL().toString(),
                connection.getResponseCode(),
                connection.getResponseMessage(),
                error);

        throw new GnipConnectionException(message);
    }
}
