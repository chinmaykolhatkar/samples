/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    ProcessOperator processor = dag.addOperator("Processor", ProcessOperator.class);
    RESTServerModule server = dag.addModule("Server", RESTServerModule.class);
    dag.addStream("toProcessor", server.request, processor.in);
    dag.addStream("toClient", processor.out, server.response);
  }
}
