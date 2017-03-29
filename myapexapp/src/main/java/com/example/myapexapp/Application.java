/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import org.apache.hadoop.conf.Configuration;

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
    RandomNumberGenerator randomGenerator1 = dag.addOperator("randomGenerator1", RandomNumberGenerator.class);
    randomGenerator1.setNumTuples(500);

    RandomNumberGenerator randomGenerator2 = dag.addOperator("randomGenerator2", RandomNumberGenerator.class);
    randomGenerator2.setNumTuples(500);

    TestOperator1 test = dag.addOperator("test", TestOperator1.class);

//    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());

//    dag.addStream("randomData", randomGenerator.out, cons.input, test.in1).setLocality(Locality.CONTAINER_LOCAL);

    dag.addStream("randomData1", randomGenerator1.out, test.in1).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("randomData2", randomGenerator2.out, test.in2).setLocality(Locality.CONTAINER_LOCAL);

  }
}
