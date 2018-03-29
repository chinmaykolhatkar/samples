package com.datatorrent.spark.datagen;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by chinmay on 24/3/18.
 */
public class SparkStreamingApp
{
  private static String oasTopic = "oas";

  public static void main(String[] args) throws Exception
  {
    SparkConf conf = new SparkConf().setAppName("oas-datagen");
    JavaStreamingContext jsc = new JavaStreamingContext(conf, Duration.apply(1000));
    JavaReceiverInputDStream<FraudPojo> pojo = jsc.receiverStream(new FraudDataReceiverStream(1000));

    String s = readSchema();
    AOO aoo = new AOO(getKafkaProperties(), oasTopic, "fraud", s);
    aoo.produce(pojo);

    jsc.start();
    jsc.awaitTermination();
  }

  private static String readSchema() throws IOException
  {
    List<String> retVal = new ArrayList<>();
    InputStream in = SparkStreamingApp.class.getResourceAsStream("/druidSchema");
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String s;
    while ((s = br.readLine()) != null) {
      retVal.add(s);
    }

    StringBuilder sb = new StringBuilder();
    for (String s1 : retVal) {
      sb.append(s1).append("\n");
    }

    return sb.toString();
  }


  private static Properties getKafkaProperties()
  {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return props;
  }
}
