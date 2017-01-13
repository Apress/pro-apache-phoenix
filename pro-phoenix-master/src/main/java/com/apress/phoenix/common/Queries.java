package com.apress.phoenix.common;

import com.google.common.io.Resources;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 *
 */
public class Queries {

    private Queries() {/* singleton */ }

    /**
     * reads the sql from the classpath available at {@code src/main/resources}
     * @param fileName
     * @return the contents of the file at {@code src/main/resources}
     */
    public static String sql(final String fileName) {
        return sql(fileName, StandardCharsets.UTF_8);
    }

    /**
     * reads the sql from the classpath available at {@code src/main/resources}
     * @param fileName
     * @return the contents of the file at {@code src/main/resources} in sql format
     */
    public static String sql(final String fileName, final Charset charSet) {
        try {
            return Resources.toString(Resources.getResource(fileName), charSet).trim();
        } catch (IOException ioException) {
            throw new IllegalArgumentException(ioException);
        }
    }
}
