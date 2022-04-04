package com.autostreams.utils.fileutils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class, providing utility methods related to loading from files.
 *
 * @version 1.0
 * @since 1.0
 */
public final class FileUtils {
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    /**
     * Private constructor for file utils to prevent instantiation of the class.
     */
    private FileUtils() {
    }

    /**
     * Loads properties from the specified config file into a properties object.
     *
     * @param configName name of the file toa load from
     * @return Properties object containing the loaded properties (if any)
     */
    public static Properties loadPropertiesFromFile(String configName) {
        Properties props = new Properties();

        try (InputStream is = ClassLoader.getSystemResourceAsStream(configName)) {
            props.load(is);
        } catch (IOException e) {
            logger.warn("Failed to load the properties from file");
            e.printStackTrace();
        }

        return props;
    }
}