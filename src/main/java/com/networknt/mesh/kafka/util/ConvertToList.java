package com.networknt.mesh.kafka.util;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import tools.jackson.core.JacksonException;
import java.util.List;
import java.util.Map;

public class ConvertToList {

    private ConvertToList(){}

	public static List<Map<String, Object>> string2List(ObjectMapper objectMapper,String s) {
        try {
            return objectMapper.readValue(s, new TypeReference<List<Map<String, Object>>>(){});
        } catch (JacksonException e) {
            throw new RuntimeException(e);
        }
    }
}
