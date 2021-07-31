package com.example.sparkkafkademo;

import com.example.UserDTO;
import kafka.Kafka;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class SparkDemo {
//    public static void main(String[] args) throws InterruptedException {
//        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "localhost:9092");
//        kafkaParams.put("key.deserializer", StringDeserializer.class);
//        kafkaParams.put("value.deserializer", StringDeserializer.class);
//        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
//        kafkaParams.put("auto.offset.reset", "latest");
//        kafkaParams.put("enable.auto.commit", false);
//
//        Collection<String> topics = Arrays.asList("messages");
//
//        SparkConf sparkConf = new SparkConf();
//        sparkConf.setMaster("spark://192.168.93.1:7077");
//        sparkConf.setAppName("SparkDemo");
//
//        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
//
//        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream
//                (streamingContext,
//                        LocationStrategies.PreferConsistent(),
//                        ConsumerStrategies.Subscribe(topics, kafkaParams));
//
//        JavaPairDStream<String, String> results = messages.mapToPair
//                (record -> new Tuple2<>(record.key(), record.value()));
//
//        JavaDStream<String> lines = results.map(Tuple2::_2);
//
//        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\s+"))
//                .iterator());
//
//        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
//                .reduceByKey(Integer::sum);
//
//        wordCounts.foreachRDD(javaRdd -> {
//            Map<String, Integer> wordCountMap = javaRdd.collectAsMap();
//            for (String key : wordCountMap.keySet()) {
//                List<Word> wordList = Arrays.asList(new Word(key, wordCountMap.get(key)));
//                JavaRDD<Word> rdd = streamingContext.sparkContext()
//                        .parallelize(wordList);
//                BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\text.txt", true));
//                writer.append(' ');
//                writer.append(key);
//
//                writer.close();
//                System.out.println(rdd);
//            }
//        });
//
//        streamingContext.start();
//        streamingContext.awaitTermination();
//    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", UserDtoDecoder.class);
        kafkaParams.put("group.id", "userDTOGROUP");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("messages");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("SparkDemo");
        BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\text.txt", true));
        writer.append("i work");
        writer.close();
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        JavaInputDStream<ConsumerRecord<String, UserDTO>> messages = KafkaUtils.createDirectStream
                (streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));

        messages.foreachRDD(javaRdd -> {
            javaRdd.foreach(record->{
                BufferedWriter writer2 = new BufferedWriter(new FileWriter("D:\\text.txt", true));
                writer2.append("\n");
                writer2.append(record.value().toString());
                writer2.close();
            });

        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
