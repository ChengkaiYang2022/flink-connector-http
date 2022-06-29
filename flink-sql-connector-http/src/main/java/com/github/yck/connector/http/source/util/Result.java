package com.github.yck.connector.http.source.util;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class Result<T> {
    private int code;
    private ObjectMapper objectMapper = new ObjectMapper();
    private String message;
    private T data;
    private Map<String, Object> result = new HashMap<String, Object>();
    private String jsonString;
    @Override
    public String toString() {

        try {
            jsonString = objectMapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            jsonString = e.getMessage();
        }
        return jsonString;
    }

    public int getCode() {
        return code;
    }

    public Result setCode(ResultCode resultCode) {
        this.code = resultCode.code();
        result.put("code",this.code);
        return this;
    }
    public Result setCode(Integer resultCode) {
        this.code = resultCode;
        result.put("code",this.code);
        return this;
    }

    public String getMessage() {
        return message;
    }

    public Result setMessage(String message) {
        this.message = message;
        this.result.put("message",this.message);
        return this;
    }

    public T getData() {
        return data;
    }

    public Result setData(T data) {
        this.data = data;
        return this;
    }
}
