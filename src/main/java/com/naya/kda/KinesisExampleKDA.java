package com.naya.kda;

import com.naya.avro.EventAttributes;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

public class KinesisExampleKDA {
    private static final String REGION = "us-east-1";

    public static void main(String[] args) throws Exception {
        Properties consumerConfig = new Properties();
        consumerConfig.put(AWSConfigConstants.AWS_REGION, REGION);
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(50000);

        DataStream<EventAttributes> consumerStream = env.addSource(new FlinkKinesisConsumer<>(
                "dev-events", new KinesisSerializer(), consumerConfig));

        consumerStream
                .addSink(getProducer());

        env.execute("kinesis-example");

    }

    private static FlinkKinesisProducer<EventAttributes> getProducer(){
        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, REGION);
        outputProperties.setProperty("AggregationEnabled", "false");

        FlinkKinesisProducer<EventAttributes> sink = new FlinkKinesisProducer<>(new KinesisSerializer(), outputProperties);
        sink.setDefaultStream("dev-result");
        sink.setDefaultPartition("0");
        return sink;
    }
}

class KinesisSerializer implements DeserializationSchema<EventAttributes>, SerializationSchema<EventAttributes> {
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
