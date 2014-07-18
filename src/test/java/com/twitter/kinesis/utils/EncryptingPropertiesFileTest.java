package com.twitter.kinesis.utils;

import com.twitter.kinesis.utils.EncryptingPropertiesFile;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.*;


public class EncryptingPropertiesFileTest {

    @Test
    public void testSomeEncryptedData() throws IOException {
        Set<String> data = new HashSet<>();
        data.add("foo");
        driveTest("someEncrypted.props", data);
    }

    @Test
    public void testNoEncryptedData() throws IOException {
        driveTest("noneEncrypted.props", new HashSet<String>());
    }

    @Test
    public void testIsDecryptable() throws IOException {
        String fileName = "toDecrypt.properties";
        try {
            Set<String> toEncrypt = new HashSet<>();
            toEncrypt.add("foo");
            FileWriter fw = new FileWriter(fileName);
            fw.write("foo=bar\n");
            fw.write("bar=foo\n");
            fw.write("other=something else\n");
            fw.close();
            EncryptingPropertiesFile props = new EncryptingPropertiesFile(toEncrypt, fileName);
            String value = props.getProperty("foo");
            assertEquals("bar", value);
            // Make sure it's still decryptable when
            // loading from file
            props = new EncryptingPropertiesFile(toEncrypt, fileName);
            value = props.getProperty("foo");
            assertEquals("bar", value);
        } finally {
            File f = new File(fileName);
            f.delete();
        }
    }

    public void driveTest(String fileName, Set<String> toEncrypt) {

        File f = new File(fileName);
        if (f.exists()) {
            f.delete();
        }
        try {

            FileWriter fw = new FileWriter(fileName);
            fw.write("foo=bar\n");
            fw.write("bar=foo\n");
            fw.write("other=something else\n");
            fw.close();
            EncryptingPropertiesFile props = new EncryptingPropertiesFile(toEncrypt, fileName);

            assertEncrypted(fileName, toEncrypt);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            if (f.exists()) {
                f.delete();
            }
        }

    }

    private void assertEncrypted(String propertyFileName, Set<String> toEncrypt) throws IOException {
        Properties props = new Properties();
        FileReader reader = new FileReader(propertyFileName);
        props.load(reader);
        reader.close();
        for (String propertyName : props.stringPropertyNames()) {
            String propertyValue = props.getProperty(propertyName);
            if (toEncrypt.contains(propertyName)) {
                assertTrue("Property: " + propertyName + " should be encrytped", propertyValue.startsWith("ENC"));
            } else {
                assertFalse("Property: " + propertyName + " should not be encrytped", propertyValue.startsWith("ENC"));
            }
        }
    }
}
