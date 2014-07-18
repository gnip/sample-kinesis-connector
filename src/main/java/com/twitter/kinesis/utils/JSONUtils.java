package com.twitter.kinesis.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JSONUtils {
    static final ThreadLocal<ObjectMapper> objectMapperThreadLocal = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper;
        }
    };

    public static ObjectMapper getObjectMapper() {
        return objectMapperThreadLocal.get();
    }

    public static JsonNode parseTree(String json) throws IOException {
        if (json == null) {
            throw new IllegalArgumentException("Input string cannot be null");
        } else {
            return getObjectMapper().readTree(json);
        }
    }
}
