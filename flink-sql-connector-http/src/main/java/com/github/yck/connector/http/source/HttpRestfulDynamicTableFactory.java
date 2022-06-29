package com.github.yck.connector.http.source;

import com.github.yck.connector.http.source.json.DeserializationRestfulFormatFactory;
import com.github.yck.connector.http.source.json.DeserializationRestfulSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

public class HttpRestfulDynamicTableFactory implements DynamicTableSourceFactory {

    // define all options statically
    public static final ConfigOption<String> PATH =
            ConfigOptions.key("path").stringType().noDefaultValue();

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port").intType().noDefaultValue();

    public static final ConfigOption<Integer> BYTE_DELIMITER =
            ConfigOptions.key("byte-delimiter").intType().defaultValue(10); // corresponds to '\n'

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
        final DecodingFormat<DeserializationRestfulSchema> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationRestfulFormatFactory.class, FactoryUtil.FORMAT);

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
        options.add(PATH);
        options.add(PORT);
        options.add(FactoryUtil.FORMAT); // use pre-defined option for format
        return options;    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BYTE_DELIMITER);
        return options;    }
}
