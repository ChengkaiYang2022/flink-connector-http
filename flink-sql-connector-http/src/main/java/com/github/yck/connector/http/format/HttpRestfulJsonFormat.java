/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.yck.connector.http.format;

import com.github.yck.connector.http.format.json.HttpRestfulJsonDeserializer;
import com.github.yck.connector.http.format.json.HttpRestfulJsonSerializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

/**
 * The {@link HttpRestfulJsonFormat} is a decoding format that uses a {@link DeserializationSchema}
 * during runtime. It supports emitting {@code INSERT} and {@code DELETE} changes.
 */
public final class HttpRestfulJsonFormat implements DecodingFormat<HttpRestfulJsonDeserializer>, EncodingFormat<HttpRestfulJsonSerializer> {

    private final String headers;

    public HttpRestfulJsonFormat(String headers) {
        this.headers = headers;
    }

    @Override
    public HttpRestfulJsonDeserializer createRuntimeDecoder(
            DynamicTableSource.Context context, DataType producedDataType) {
        // create type information for the DeserializationSchema
        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        // most of the code in DeserializationSchema will not work on internal data structures
        // create a converter for conversion at the end
        final DataStructureConverter converter =
                context.createDataStructureConverter(producedDataType);

        // use logical types during runtime for parsing
        final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();

        // create runtime class
        return new HttpRestfulJsonDeserializer(
                parsingTypes, converter, producedTypeInfo, headers);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // define that this format can produce INSERT and DELETE rows
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public HttpRestfulJsonSerializer createRuntimeEncoder(DynamicTableSink.Context context, DataType producedDataType) {
        // create type information for the DeserializationSchema
        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        // most of the code in DeserializationSchema will not work on internal data structures
        // create a converter for conversion at the end
        final DynamicTableSink.DataStructureConverter converter =
                context.createDataStructureConverter(producedDataType);

        // use logical types during runtime for parsing
        final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();

        // create runtime class
        return new HttpRestfulJsonSerializer(
                parsingTypes, converter, producedTypeInfo,headers);
    }
}
