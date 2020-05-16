package com.yr.boot;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author dengbp
 * @ClassName BootstrapApplication
 * @Description TODO
 * @date 2020-04-16 19:00
 */
public class BootstrapApplication {
   static Logger log = LoggerFactory.getLogger(BootstrapApplication.class);
    public static void main(final String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /** 多个broker 配置：node002:9200,node002:9201 */
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "node002:9092");
        /** 500s commit一次 */
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("license-service-logs-topic");
        textLines.foreach((k,v)->log.info("k={},v={}",k,v));


//        KTable<String, Long> wordCounts = textLines
//                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
//                .groupBy((key, word) -> word)
//                .count();
//        /** 写回topic */
//        wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));
        /** 打印控制台 */
//        wordCounts.toStream().print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Thread.currentThread().join();
    }

}
