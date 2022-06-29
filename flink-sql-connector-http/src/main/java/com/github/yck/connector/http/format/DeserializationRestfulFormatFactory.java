package com.github.yck.connector.http.format;

import com.github.yck.connector.http.format.json.DeserializationRestfulSchema;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.factories.DecodingFormatFactory;
@PublicEvolving
public interface DeserializationRestfulFormatFactory extends DecodingFormatFactory<DeserializationRestfulSchema> {
}
