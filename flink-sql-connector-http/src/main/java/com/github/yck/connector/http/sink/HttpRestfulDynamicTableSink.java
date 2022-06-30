package com.github.yck.connector.http.sink;

import com.github.yck.connector.http.format.json.HttpRestfulJsonSerializer;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Map;

public class HttpRestfulDynamicTableSink implements DynamicTableSink {
    private String remoteUrl;
    private final EncodingFormat<HttpRestfulJsonSerializer> encodingFormat;
    private final DataType producedDataType;

    public HttpRestfulDynamicTableSink(String remoteUrl, EncodingFormat<HttpRestfulJsonSerializer> encodingFormat, DataType producedDataType, Map<String, String> headers) {
        this.remoteUrl = remoteUrl;
        this.encodingFormat = encodingFormat;
        this.producedDataType = producedDataType;
        this.headers = headers;
    }

    private Map<String, String> headers;
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // TODO changeLog should be build in some se
        return  ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final HttpRestfulJsonSerializer serializer =
                encodingFormat.createRuntimeEncoder(context, producedDataType);

        HttpRestfulSinkFunction sinkFunction = new HttpRestfulSinkFunction(
                remoteUrl,
                headers,
                serializer);
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new HttpRestfulDynamicTableSink(remoteUrl, encodingFormat, producedDataType, headers);
    }

    @Override
    public String asSummaryString() {
        return "Http Restful Table Sink";
    }
}
