package com.twitter.kinesis.perftest;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;

public class Configure {
    public static String propFile = "decrypted.properties";

    public static ClasspathPropertiesFileCredentialsProvider getAwsCredentials() {
        return new ClasspathPropertiesFileCredentialsProvider(Configure.propFile);
    }
}
