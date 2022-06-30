package com.github.yck.connector.http.format.json;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HttpRestfulJsonSerializer implements SerializationSchema<RowData> {
    public HttpRestfulJsonSerializer(List<LogicalType> parsingTypes, DynamicTableSink.DataStructureConverter converter, TypeInformation<RowData> producedTypeInfo,String headers) {
        this.headers = headers;
        this.parsingTypes = parsingTypes;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
    }
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
    }
    private final String headers;
    private final List<LogicalType> parsingTypes;
    private final DynamicTableSink.DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;
    /**
     * @param element
     *
     * @return
     */
    private void addHeaders(RowData element){

    }

    @Override
    public byte[] serialize(RowData element) {
        return new byte[0];
    }

    /**TODO Add body from setup
     * @param httpPost
     * @param headers
     * @return
     */
    private HttpPost addHeaders(HttpPost httpPost, Map<String ,String> headers){

        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json; charset=UTF-8");

        return httpPost;
    }

    /**
     * @param
     * @return
     */
    private String addRequestBody(RowData rowData){
        Row element = (Row) converter.toExternal(rowData);
        System.out.println(element);
        ObjectNode body = objectMapper.createObjectNode();
        for (String fieldName : element.getFieldNames(true)) {
            body.put(fieldName, String.valueOf(element.getField(fieldName)));
        }
        return body.toPrettyString();
    }
    public HttpPost serializeToHTTPPost(RowData element, String remoteUrl, Map<String,String> headers){

        HttpPost httpPost = new HttpPost(remoteUrl);
        addHeaders(element);
        httpPost.setEntity(new StringEntity(addRequestBody(element), "UTF-8"));

        return httpPost;
    }
}
