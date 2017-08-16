package com.example.myapexapp;

import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Created by chinmay on 3/8/17.
 */
public class QueryResponder extends BaseOperator
{
  private Map<Long, RESTManager.RequestInfo> requestInfoMap = Maps.newHashMap();

  public transient final DefaultInputPort<KeyValPair<Long, String>> response = new DefaultInputPort<KeyValPair<Long, String>>()
  {
    @Override
    public void process(KeyValPair<Long, String> tuple)
    {
      processResponse(tuple);
    }
  };

  public transient final DefaultInputPort<RESTManager.RequestInfo> requestInfo = new DefaultInputPort<RESTManager.RequestInfo>()
  {
    @Override
    public void process(RESTManager.RequestInfo tuple)
    {
      requestInfoMap.put(tuple.requestId, tuple);
    }
  };

  private void processResponse(KeyValPair<Long, String> tuple)
  {

    RESTManager.RequestInfo requestInfo = requestInfoMap.get(tuple.getKey());
    if (requestInfo == null) {
      System.out.println("Huh!!!");
      return;
    }

    requestInfo.response.handle(tuple.getValue());
  }
}
