package com.github.yck.connector.http;

import com.github.yck.connector.http.format.json.HttpRestfulJsonDeserializer;
import com.github.yck.connector.http.sink.HttpRestfulDynamicTableSink;
import com.github.yck.connector.http.source.HttpRestfulDynamicTableSource;
import com.github.yck.connector.http.format.DecodingHttpRestfulFormatFactory;
import com.github.yck.connector.http.format.json.HttpRestfulJsonSerializer;
import com.github.yck.connector.http.format.EncodingHttpRestfulFormatFactory;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * TODO Add headers
 */
public class HttpRestfulDynamicTableFactory implements DynamicTableSinkFactory,DynamicTableSourceFactory {

    // define all options statically
    public static final ConfigOption<String> PATH =
            ConfigOptions.key("path").stringType().noDefaultValue();

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port").intType().noDefaultValue();

    public static final ConfigOption<Integer> BYTE_DELIMITER =
            ConfigOptions.key("byte-delimiter").intType().defaultValue(10); // corresponds to '\n'

    public static final ConfigOption<String> REMOTE_URL =
            ConfigOptions.key("remote-url").stringType().noDefaultValue();

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
        final DecodingFormat<HttpRestfulJsonDeserializer> decodingFormat =
                helper.discoverDecodingFormat(
                        DecodingHttpRestfulFormatFactory.class, FactoryUtil.FORMAT);

        // validate all options
        helper.validate();

        // get the validated options
        final ReadableConfig options = helper.getOptions();
        final String path = options.get(PATH);
        final int port = options.get(PORT);
        final byte byteDelimiter = (byte) (int) options.get(BYTE_DELIMITER);

        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        // create and return dynamic table source
        return new HttpRestfulDynamicTableSource(
                path, port, byteDelimiter, decodingFormat, producedDataType);
    }

    @Override
    public String factoryIdentifier() {
        return "http-restful";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
//        options.add(PATH);
//        options.add(PORT);
        options.add(FactoryUtil.FORMAT); // use pre-defined option for format
        return options;    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        options.add(PORT);
        options.add(REMOTE_URL);
        options.add(BYTE_DELIMITER);
        return options;    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
        final EncodingFormat<HttpRestfulJsonSerializer> encodingFormat =
                helper.discoverEncodingFormat(
                        EncodingHttpRestfulFormatFactory.class, FactoryUtil.FORMAT);

        // validate all options
        helper.validate();

        // get the validated options
        final ReadableConfig options = helper.getOptions();
        final String remoteUrl = options.get(REMOTE_URL);
        // TODO Get Header Map from configurations.
        HashMap<String, String> headers = new HashMap<>();

        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        // create and return dynamic table source
        return new HttpRestfulDynamicTableSink(
                remoteUrl, encodingFormat, producedDataType, headers);
    }
}
