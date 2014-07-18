package com.twitter.kinesis.utils;

import org.jasypt.encryption.StringEncryptor;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.PropertyValueEncryptionUtils;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

public class EncryptingPropertiesFile extends Properties {

    StringEncryptor stringEncryptor;
    private Set<String> propertiesToEncrypt;
    boolean isDirty;

    public EncryptingPropertiesFile(Set<String> propertiesToEncrypt, String filename) throws IOException {
        this.stringEncryptor = getStringEncryptor();
        this.propertiesToEncrypt = propertiesToEncrypt;
        loadProperties(filename);
        if (isDirty) {
            writeEncryptedProperties(filename);
        }
    }

    private static StringEncryptor getStringEncryptor() {
        StandardPBEStringEncryptor stringEncryptor = new StandardPBEStringEncryptor();
        stringEncryptor.setPassword("S0up3rS3cr3t");
        stringEncryptor.initialize();
        return stringEncryptor;
    }

    private void writeEncryptedProperties(String filename) throws IOException {
        FileWriter writer = new FileWriter(filename);
        this.store(writer, "");
        writer.close();
        isDirty = false;
    }

    private void loadProperties(String filename) throws IOException {
        Properties props = new Properties();
        FileReader reader = new FileReader(filename);
        props.load(reader);
        reader.close();
        for (String propertyName : props.stringPropertyNames()) {
            String propertyValue = props.getProperty(propertyName);
            this.setProperty(propertyName, propertyValue);
        }

    }

    @Override
    public String getProperty(String key) {
        String value = super.getProperty(key);
        if (value != null) {
            if (propertiesToEncrypt.contains(key) && PropertyValueEncryptionUtils.isEncryptedValue(value)) {
                value =PropertyValueEncryptionUtils.decrypt(value,stringEncryptor);
            }
        }
        return value;
    }

    @Override
    public synchronized Object setProperty(String key, String value) {
        if (propertiesToEncrypt.contains(key) && !PropertyValueEncryptionUtils.isEncryptedValue(value)) {
            isDirty = true;
            value = PropertyValueEncryptionUtils.encrypt(value, stringEncryptor);
        }
        return super.setProperty(key, value);
    }
}
