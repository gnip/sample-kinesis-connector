package com.twitter.kinesis.connection;

import com.twitter.kinesis.connection.UriStrategy;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;

public class UriStrategyTest {

    UriStrategy strategy ;

    @Before
    public void setup() {
        strategy = new UriStrategy();
    }

    @Test
    public void testCreateUriNoClient() {
        URI streamUri = strategy.createStreamUri("account", "publisher", "product", "label", null);
        assertEquals ("https://stream.gnip.com:443/accounts/account/publishers/publisher/streams/product/label.json", streamUri.toString());
    }

    @Test
    public void testCreateUriWithClient() {
        URI streamUri = strategy.createStreamUri("account", "publisher", "product", "label", "111");
        assertEquals ("https://stream.gnip.com:443/accounts/account/publishers/publisher/streams/product/label.json?client=111", streamUri.toString());
    }
}
