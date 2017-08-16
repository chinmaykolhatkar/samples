package com.example.myapexapp;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.esotericsoftware.kryo.NotNull;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Created by chinmay on 3/8/17.
 */
public class QueryReceiver extends BaseOperator implements InputOperator
{
  public transient final DefaultOutputPort<KeyValPair<Long, String>> query = new DefaultOutputPort<>();
  public transient final DefaultOutputPort<RESTManager.RequestInfo> requestInfo = new DefaultOutputPort<>();

  @AutoMetric
  private String address;

  @NotNull
  private String resourcePackages;

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      RESTManager.getInstance().startServlet(resourcePackages.split(","));
      address = RESTManager.getInstance().getPort();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
    try {
      RESTManager.getInstance().stopServlet();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void emitTuples()
  {
    try {
      RESTManager.RequestInfo poll = RESTManager.getInstance().getRequestQueue().poll(10, TimeUnit.MILLISECONDS);
      if (poll == null) {
        return;
      }
      query.emit(new KeyValPair<>(poll.requestId, poll.request));
      requestInfo.emit(poll);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
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
