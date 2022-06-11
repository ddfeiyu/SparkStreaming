package cn.just.spark.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JsonUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        // Include.NON_EMPTY 属性为 空（""） 或者为 NULL 都不序列化，则返回的json是没有这个字段的。这样对移动端会更省流量
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 允许出现特殊字符和转义符
        MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        // 允许出现单引号
        MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        // 字段保留，将null值转为""
        MAPPER.getSerializerProvider().setNullValueSerializer(new JsonSerializer<Object>() {
            @Override
            public void serialize(Object o, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
                jsonGenerator.writeString("");
            }
        });
    }

    public static JsonNode stringToJson(String content) {
        try {
            JsonNode jsonNode = MAPPER.readTree(content);
            return jsonNode;
        } catch (IOException e) {
            // log.error("objectToJson error:", e);
        }
        return null;
    }

    public static int[] readIntArray(JsonNode node) {
        int size = node.size();
        int[] arr = new int[size];
        Iterator<JsonNode> iterator = node.iterator();
        int idx = 0;
        while (iterator.hasNext()) {
            JsonNode item = iterator.next();
            arr[idx++] = item.asInt();
        }
        return arr;
    }

    public static long[] readLongArray(JsonNode node) {
        int size = node.size();
        long[] arr = new long[size];
        Iterator<JsonNode> iterator = node.iterator();
        int idx = 0;
        while (iterator.hasNext()) {
            JsonNode item = iterator.next();
            arr[idx++] = item.asLong();
        }
        return arr;
    }

    public static String objectToJson(Object data) {
        try {
            String result = MAPPER.writeValueAsString(data);
            return result;
        } catch (JsonProcessingException e) {
            // log.error("objectToJson error:", e);
        }
        return null;
    }

    public static <T> T jsonToBean(String jsonData, Class<T> beanType) {
        try {
            T result = MAPPER.readValue(jsonData, beanType);
            return result;
        } catch (Exception e) {
            // log.error("jsonToBean error:", e);
        }

        return null;
    }

    /**
     * 字符串转Java对象（自动将下换线转换为驼峰）
     *
     * @param jsonData
     * @param beanType
     * @param <T>
     * @return
     */
    public static <T> T jsonToBeanExt(String jsonData, Class<T> beanType) {
        PropertyNamingStrategy propertyNamingStrategy = MAPPER.getPropertyNamingStrategy();
        try {
            MAPPER.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
            T result = MAPPER.readValue(jsonData, beanType);
            return result;
        } catch (Exception e) {
            // log.error("jsonToBean error:", e);
        } finally {
            MAPPER.setPropertyNamingStrategy(propertyNamingStrategy);
        }
        return null;
    }

    public static <T> List<T> jsonToList(String jsonData, Class<T> beanType) {
        JavaType javaType = MAPPER.getTypeFactory().constructParametricType(List.class, beanType);

        try {
            List<T> resultList = MAPPER.readValue(jsonData, javaType);
            return resultList;
        } catch (Exception e) {
            // log.error("jsonToList error:", e);
        }

        return null;
    }

    public static <K, V> Map<K, V> jsonToMap(String jsonData, Class<K> keyType, Class<V> valueType) {
        JavaType javaType = MAPPER.getTypeFactory().constructMapType(Map.class, keyType, valueType);

        try {
            Map<K, V> resultMap = MAPPER.readValue(jsonData, javaType);
            return resultMap;
        } catch (Exception e) {
            // log.error("jsonToMap error:", e);
        }

        return null;
    }
    public static <K, V> Map<K, V> jsonToMap(String jsonData, TypeReference<Map<K, V>> valueTypeRef) {

        try {
            Map<K, V> resultMap = MAPPER.readValue(jsonData, valueTypeRef);
            return resultMap;
        } catch (Exception e) {
            // log.error("jsonToMap error:", e);
        }

        return null;
    }
}
