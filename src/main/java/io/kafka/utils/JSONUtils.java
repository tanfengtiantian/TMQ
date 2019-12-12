package io.kafka.utils;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
public class JSONUtils {

    public static String serializeObject(final Object o) throws Exception {
        return mapper.writeValueAsString(o);
    }

    static ObjectMapper mapper = new ObjectMapper();


    public static Object deserializeObject(final String s, final Class<?> clazz) throws Exception {
        return mapper.readValue(s, clazz);
    }


    public static Object deserializeObject(final String s, final TypeReference<?> typeReference) throws Exception {
        return mapper.readValue(s, typeReference);
    }
}