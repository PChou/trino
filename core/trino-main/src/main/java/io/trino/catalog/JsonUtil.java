/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.catalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/05 15:36
 * @since 1.0.0
 */
public class JsonUtil
{

    private static final Logger log = Logger.get(ZookeeperCatalogStoreConfig.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonUtil() {
        throw new IllegalStateException("util class");
    }

    public static <T> T toObj(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            log.error("toObj error");
            throw ZookeeperCatalogException.newInstance("string to object error", e);
        }
    }

    public static <T> T toObjNoException(T json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue((String) json, clazz);
        } catch (IOException e) {
            log.warn("toObjNoException error");
            return json;
        }
    }

    public static <T> T toObj(String json, TypeReference<T> typeReference) {
        try {
            return OBJECT_MAPPER.readValue(json, typeReference);
        } catch (IOException e) {
            log.error("toObj error");
            throw ZookeeperCatalogException.newInstance("string to object error", e);
        }
    }

    public static <T> List<T> toArray(String json, Class<T> clazz) {
        try {
            CollectionType type = OBJECT_MAPPER.getTypeFactory().constructCollectionType(ArrayList.class, clazz);
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            log.error("toArray error");
            throw ZookeeperCatalogException.newInstance("string to list error", e);
        }
    }

    public static <T> String toJson(T obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (IOException e) {
            log.error("toJson error");
            throw ZookeeperCatalogException.newInstance("object to string error", e);
        }
    }
}
