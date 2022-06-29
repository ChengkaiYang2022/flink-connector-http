package com.github.yck.connector.http.sink;
import com.github.yck.connector.http.format.json.HttpRestfulJsonSerializer;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.Map;

public class HttpRestfulSinkFunction extends RichSinkFunction<RowData> {
    private String remoteUrl;
    private Map<String, String> headers;
    private CloseableHttpClient client;
    private final HttpRestfulJsonSerializer serializer;

    public HttpRestfulSinkFunction(String remoteUrl, Map<String, String> headers, HttpRestfulJsonSerializer serializer) {
        this.remoteUrl = remoteUrl;
        this.headers = headers;
        this.serializer = serializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        System.out.println(parameters.toString());
        client = HttpClients.createDefault();

    }

    @Override
    public void close() throws Exception {
        client.close();
    }


    @Override
    public void invoke(RowData value, Context context) throws Exception {
        HttpPost httpPost = new HttpPost(remoteUrl);
        HttpDelete httpDelete = new HttpDelete(remoteUrl);
//        httpDelete.();
        // TODO add a serialize convert RowData to a request with POST/DELETE method.

        serializer.serializeToJson(value);

        StringEntity entity = new StringEntity("{\"code\":200,\"message\":\"success\"}", "UTF-8");
        httpPost.setEntity(entity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json; charset=UTF-8");

        CloseableHttpResponse response = null;
        response = client.execute(httpPost);
        String responseBody = EntityUtils.toString(response.getEntity());
        System.out.println(responseBody);
    }

    @Override
    public void finish() throws Exception {
        super.finish();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return null;
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return null;
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {

    }
}
