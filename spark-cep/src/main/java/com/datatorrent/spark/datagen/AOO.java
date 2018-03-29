package com.datatorrent.spark.datagen;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.codehaus.jackson.map.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Created by chinmay on 25/3/18.
 */
public class AOO<V> implements Serializable
{
  private String dsName;
  private String schema;
  private String topic;
  private Properties props;

  public AOO(Properties p, String topic, String dsName, String schema) throws IOException
  {
    this.props = p;
    this.topic = topic;
    this.dsName = dsName;
    this.schema = schema;
  }

  public String ser(Object o) throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(o);
  }

  public void produce(JavaDStream<V> stream)
  {
    stream.map(new Function<V, String>()
    {
      @Override
      public String call(V v) throws Exception
      {
        AnalyticsTuple tuple = new AnalyticsTuple(dsName, ser(v), AnalyticsTuple.TupleType.DATA);
        return ser(tuple);
      }
    })
      .foreachRDD(new VoidFunction<JavaRDD<String>>()
      {
        @Override
        public void call(JavaRDD<String> rdd) throws Exception
        {
          Producer<String, String> p = getProducer();
          AnalyticsTuple s = new AnalyticsTuple(dsName, schema, AnalyticsTuple.TupleType.ADD_DATA_SOURCE);
          p.send(new ProducerRecord<String, String>(topic, ser(s)));
          p.close();

          rdd.foreachPartition(new VoidFunction<Iterator<String>>()
          {
            @Override
            public void call(Iterator<String> it) throws Exception
            {
              Producer<String, String> producer = getProducer();
              while (it.hasNext()) {
                producer.send(new ProducerRecord<String, String>(topic, it.next()));
              }
            }
          });
        }
      });
  }

  private Producer<String, String> getProducer()
  {
    return new KafkaProducer<>(props);
  }
}
