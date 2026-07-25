package com.venky.demos.kstream.util;


import java.lang.reflect.Field;
import java.util.List;

public class NullReplacementUtil {

    /**
     * Recursively loops through all fields of a given object instance and
     * switches any 'null' string values out for an empty string ("").
     */
    public static <T> T replaceNullsWithEmptyStrings(T obj) {
        if (obj == null) {
            return obj;
        }
        try {
            Field[] fields = obj.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);

                if (field.getType().equals(String.class)) {
                    String value = (String) field.get(obj);
                    if (value == null) {
                        field.set(obj, "");
                    }
                } else if (!field.getType().isPrimitive() &&
                        !field.getType().equals(List.class) &&
                        field.getType().getPackage() != null &&
                        field.getType().getPackage().getName().startsWith("com.venky.demos.kstream")) {

                    Object nestedObj = field.get(obj);
                    if (nestedObj != null) {
                        replaceNullsWithEmptyStrings(nestedObj);
                    }
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to replace null values in object: " +
                    obj.getClass().getSimpleName(), e);
        }
        return obj;
    }

    /**
     * Fallback standard helper to catch single text items cleanly.
     */
    public static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
