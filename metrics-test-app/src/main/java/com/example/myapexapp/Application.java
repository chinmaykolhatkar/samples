/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
    randomGenerator.setNumTuples(500);
    dag.setAttribute(randomGenerator, Context.OperatorContext.METRICS_AGGREGATOR, new MyMetricsAggregator());

    Consumer consumer = dag.addOperator("consumer", Consumer.class);

    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());

    dag.addStream("randomData", randomGenerator.out, consumer.in, cons.input).setLocality(Locality.CONTAINER_LOCAL);

    dag.setAttribute(Context.DAGContext.METRICS_TRANSPORT, null);
  }
}
