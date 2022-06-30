package com.github.yck.connector.http.sink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

public class HttpRestfulSinkFunctionTest {
    private CloseableHttpClient client;
    private String url = "http://localhost:8080/flink/table1";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void prepare(){
        client = HttpClients.createDefault();

    }
    @Test
    public void test2(){
        ObjectNode user = objectMapper.createObjectNode();
        user.put("name","yck");
        user.put("age",28);
        System.out.println(user.toString());
    }
    @Test
    public void testPost(){
        try {

            HttpPost httpPost = new HttpPost(url);


            StringEntity entity = new StringEntity("{\"code\":200,\"message\":\"success\"}", "UTF-8");
            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json; charset=UTF-8");

            CloseableHttpResponse response = null;
            response = client.execute(httpPost);
            String responseBody = EntityUtils.toString(response.getEntity());
            System.out.println(responseBody);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}