package com.github.yck.connector.http.format.json;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

public class HttpRestfulJsonSerializer implements SerializationSchema<RowData> {
    public HttpRestfulJsonSerializer(List<LogicalType> parsingTypes, DynamicTableSink.DataStructureConverter converter, TypeInformation<RowData> producedTypeInfo) {
        this.parsingTypes = parsingTypes;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
    }
    private final List<LogicalType> parsingTypes;
    private final DynamicTableSink.DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;
    /**
     * @param element
     *
     * @return
     */
    @Override
    public byte[] serialize(RowData element) {
        return new byte[0];
    }
    public void serializeToJson(RowData element){
        System.out.println(element);
        return;
    }
}
