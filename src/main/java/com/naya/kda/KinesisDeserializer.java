package com.naya.kda;

import com.naya.avro.EventAttributes;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.introspect.TypeResolutionContext;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import java.io.IOException;
import java.nio.ByteBuffer;

public class KinesisDeserializer implements KinesisDeserializationSchema<EventAttributes> {
    @Override
    public EventAttributes deserialize(byte[] bytes, String s, String s1, long l, String s2, String s3) throws IOException {
        return EventAttributes.fromByteBuffer(ByteBuffer.wrap(bytes));
    }

    @Override
    public TypeInformation<EventAttributes> getProducedType() {
        return TypeInformation.of(EventAttributes.class);
    }
}
