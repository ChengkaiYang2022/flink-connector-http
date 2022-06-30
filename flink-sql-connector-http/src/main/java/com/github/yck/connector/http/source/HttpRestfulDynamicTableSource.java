package com.github.yck.connector.http.source;

import com.github.yck.connector.http.format.json.HttpRestfulJsonDeserializer;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class HttpRestfulDynamicTableSource implements ScanTableSource {
    private String path;
    private Integer port;
    private final byte byteDelimiter;
    private final DecodingFormat<HttpRestfulJsonDeserializer> decodingFormat;
    private final DataType producedDataType;

    public HttpRestfulDynamicTableSource(String path, int port, byte byteDelimiter, DecodingFormat<HttpRestfulJsonDeserializer> decodingFormat, DataType producedDataType) {
        this.path = path;
        this.port = port;
        this.byteDelimiter = byteDelimiter;
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        // create runtime classes that are shipped to the cluster

        final HttpRestfulJsonDeserializer deserializer =
                decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType);

        final SourceFunction<RowData> sourceFunction =
                new HttpRestfulSourceFunction(path, port, byteDelimiter, deserializer);

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new HttpRestfulDynamicTableSource(
                path, port, byteDelimiter, decodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Http Restful Table Source";
    }
}
