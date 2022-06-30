package com.github.yck.connector.http.format;

import com.github.yck.connector.http.format.json.HttpRestfulJsonDeserializer;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.factories.DecodingFormatFactory;
@PublicEvolving
public interface DeserializationRestfulFormatFactory extends DecodingFormatFactory<HttpRestfulJsonDeserializer> {
}
