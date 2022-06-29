package com.github.yck.connector.http.format;

import com.github.yck.connector.http.format.json.HttpRestfulJsonSerializer;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.factories.EncodingFormatFactory;

@PublicEvolving
public interface SerializationRestfulFormatFactory extends EncodingFormatFactory<HttpRestfulJsonSerializer> {
}
