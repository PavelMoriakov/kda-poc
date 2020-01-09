package com.naya.kda;

import com.naya.avro.EventAttributes;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.ByteBuffer;

public class KinesisSerializer implements DeserializationSchema<EventAttributes>, SerializationSchema<EventAttributes> {
    @Override
    public EventAttributes deserialize(byte[] bytes) throws IOException {
        return EventAttributes.fromByteBuffer(ByteBuffer.wrap(bytes));
    }

    @Override
    public boolean isEndOfStream(EventAttributes eventAttributes) {
        return false;
    }

    @Override
    public byte[] serialize(EventAttributes eventAttributes) {
        try {
            return eventAttributes.toByteBuffer().array();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[1];
    }

    @Override
    public TypeInformation<EventAttributes> getProducedType() {
        return TypeInformation.of(EventAttributes.class);
    }
}
