package com.github.yck.connector.http.format.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.List;

public interface DeserializationRestfulSchema extends DeserializationSchema<RowData> {
    /**
     * @param message
     * @param rowKind
     * @return
     * @throws IOException
     */
    RowData deserializeSingleJsonStringWithRowKind(byte[] message,RowKind rowKind) throws IOException;

    /**
     * 1. convert json list from a byte array(message)
     * 2. convert json list to a row list with all the rowKind (INSERT, DELETE etc.)
     * @param message
     * @param rowKind
     * @return
     * @throws IOException
     */
    List<RowData> deserializeJsonListWithRowKind(byte[] message, RowKind rowKind) throws IOException;
}
