package com.example.myapexapp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.common.util.Pair;

/**
 * Created by chinmay on 4/8/17.
 */
public class QueryReceiverAggregator implements AutoMetric.Aggregator, Serializable
{
  private static final long serialVersionUID = -1790027920120783452L;
  Map<String, Object> result = Maps.newHashMap();

  @Override
  public Map<String, Object> aggregate(long windowId, Collection<AutoMetric.PhysicalMetricsContext> physicalMetrics)
  {
    Collection<Collection<Pair<String, Object>>> ccp = new ArrayList<>();
    List<String> addresses = Lists.newArrayList();

    for (AutoMetric.PhysicalMetricsContext physicalMetric : physicalMetrics) {
      Map<String, Object> metrics = physicalMetric.getMetrics();
      if (metrics.containsKey("address")) {
        String address = (String)metrics.get("address");
        Collection<Pair<String, Object>> row = new ArrayList<>();
        row.add(new Pair<>("Address", (Object)address));
        ccp.add(row);

        addresses.add(address);
      }
    }

//    result.put("RESTServerAddress", ccp);

    result.put("RESTServerAddress", Joiner.on(",").join(addresses));
    return result;
  }
}
