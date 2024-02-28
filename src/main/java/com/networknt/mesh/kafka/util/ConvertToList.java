package com.networknt.mesh.kafka.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ConvertToList {

    private ConvertToList(){}

	public static List<Map<String, Object>> string2List(ObjectMapper objectMapper,String s) {
        try {
            return objectMapper.readValue(s, new TypeReference<List<Map<String, Object>>>(){});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
