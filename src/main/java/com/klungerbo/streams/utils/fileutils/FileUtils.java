package com.klungerbo.streams.utils.fileutils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.jetbrains.annotations.NotNull;

/**
 * Utility class, providing utility methods related to loading from files.
 *
 * @version 1.0
 * @since 1.0
 */
public final class FileUtils {
    /**
     * Private constructor for file utils to prevent instantiation of the class.
     */
    private FileUtils() {

    }

    /**
     * Loads configs from the specified config file into a properties object.
     *
     * @param configName name of the file toa load from
     * @return Properties object containing the loaded properties (if any)
     */
    public static Properties loadConfigFromFile(@NotNull String configName) throws IOException {
        Properties props = new Properties();

        InputStream is = ClassLoader.getSystemResourceAsStream(configName);
        props.load(is);

        return props;
    }
}