package com.example.myapexapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Created by chinmay on 3/8/17.
 */
public class RESTServerModule implements Module
{
  public transient ProxyInputPort<KeyValPair<Long, String>> response = new ProxyInputPort<>();
  public transient ProxyOutputPort<KeyValPair<Long, String>> request = new ProxyOutputPort<>();

  private String resourcePackages;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    QueryReceiver restReceiver = dag.addOperator("RESTReceiver", new QueryReceiver());
    restReceiver.setResourcePackages(resourcePackages);

    QueryResponder restResponder = dag.addOperator("RESTResponder", new QueryResponder());

    response.set(restResponder.response);
    request.set(restReceiver.query);
    dag.addStream("meta", restReceiver.requestInfo, restResponder.requestInfo).setLocality(DAG.Locality.CONTAINER_LOCAL);

    dag.setOperatorAttribute(restReceiver, Context.OperatorContext.METRICS_AGGREGATOR, new QueryReceiverAggregator());
  }

  public String getResourcePackages()
  {
    return resourcePackages;
  }

  public void setResourcePackages(String resourcePackages)
  {
    this.resourcePackages = resourcePackages;
  }
}
