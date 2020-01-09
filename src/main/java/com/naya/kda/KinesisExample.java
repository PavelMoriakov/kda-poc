package com.naya.kda;

import com.naya.avro.EventAttributes;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KinesisExample {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(KinesisExample.class);

    private static final String REGION = "us-east-1";

    public static void main(String[] args) throws Exception {
        Properties consumerConfig = new Properties();
        consumerConfig.put(AWSConfigConstants.AWS_REGION, REGION);
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);

       /* DataStream<EventAttributes> consumerStream = env.addSource(new FlinkKinesisConsumer<>(
                "dev-pzn-events", new KinesisDeserializer(), consumerConfig));*/
        DataStream<String> consumerStream = env.addSource(new FlinkKinesisConsumer<>(
                "dev-pzn-events", new SimpleStringSchema(), consumerConfig));

        consumerStream.addSink(getProducer());

        env.execute("test-kda");

    }

    //private static FlinkKinesisProducer<EventAttributes> getProducer(){
    private static FlinkKinesisProducer<String> getProducer(){
        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, REGION);
        outputProperties.setProperty("AggregationEnabled", "false");

        //FlinkKinesisProducer<EventAttributes> sink = new FlinkKinesisProducer<>(new KinesisSerializer(), outputProperties);
        FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(), outputProperties);
        sink.setDefaultStream("dev-pzn-behavior");
        sink.setDefaultPartition("0");
        return sink;
    }
}
