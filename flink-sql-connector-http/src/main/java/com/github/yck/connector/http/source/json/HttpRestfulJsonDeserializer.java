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

package com.github.yck.connector.http.source.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.connector.RuntimeConverter.Context;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link HttpRestfulJsonDeserializer} contains a simple parsing logic for converting bytes into
 * {@link Row} of {@link Integer} and {@link String} with a {@link RowKind}.
 *
 * <p>The final conversion step converts those into internal data structures.
 */
public final class HttpRestfulJsonDeserializer implements DeserializationRestfulSchema {

    private final List<LogicalType> parsingTypes;
    private final DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;
    private final String columnDelimiter;
    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    public HttpRestfulJsonDeserializer(
            List<LogicalType> parsingTypes,
            DataStructureConverter converter,
            TypeInformation<RowData> producedTypeInfo,
            String columnDelimiter) {
        this.parsingTypes = parsingTypes;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        // return the type information required by Flink's core interfaces
        return producedTypeInfo;
    }

    @Override
    public void open(InitializationContext context) {
        // converters must be opened
        converter.open(Context.create(HttpRestfulJsonDeserializer.class.getClassLoader()));
    }

    /**
     * TODO using deserialize to decode json list and collect row data iterable
     * @param message
     * @param out
     * @throws IOException
     */
    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        DeserializationRestfulSchema.super.deserialize(message, out);
    }

    /**
     * Decode simple json (not a json list) and return row data.
     * @param message
     * @return
     */
    @Override
    public RowData deserialize(byte[] message) throws IOException {
        // parse the columns including a changelog flag
        Map<String,Object> word = new HashMap<>();
        Row row = Row.withNames();

        word = objectMapper.readValue(message, Map.class);
        for (String s : word.keySet()) {
                row.setField(s,word.get(s));
        }

        // convert to internal data structure
        return (RowData) converter.toInternal(row);
    }

    private static Object parse(LogicalTypeRoot root, String value) {
        switch (root) {
            case INTEGER:
                return Integer.parseInt(value);
            case VARCHAR:
                return value;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public RowData deserializeSingleJsonStringWithRowKind(byte[] message, RowKind rowKind) throws IOException {
        RowData rowData = this.deserialize(message);
        rowData.setRowKind(rowKind);
        return rowData;
    }

    @Override
    public List<RowData> deserializeJsonListWithRowKind(byte[] message, RowKind rowKind) throws IOException {
        // parse the columns including a changelog flag
        List<Map<String,Object>> jsonList = new ArrayList<>();
        List<RowData> re = new ArrayList<>();

        jsonList = objectMapper.readValue(message, List.class);
        for (Map<String,Object> json : jsonList) {
            Row row = Row.withNames();
            for (String s : json.keySet()) {
                row.setField(s,json.get(s));
            }
            row.setKind(rowKind);
            re.add((RowData) converter.toInternal(row));
        }
        return re;

    }
}
