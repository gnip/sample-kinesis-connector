package com.twitter.kinesis.utils;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.google.inject.Inject;

public class CredentialsProviderChain extends AWSCredentialsProviderChain {

    @Inject
    public CredentialsProviderChain(Environment environment) {
        super(new InstanceProfileCredentialsProvider(),
                environment);
    }
}
