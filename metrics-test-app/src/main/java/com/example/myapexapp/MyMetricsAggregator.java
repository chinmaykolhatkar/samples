package com.example.myapexapp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.datatorrent.common.util.Pair;
import com.google.common.collect.Maps;

import com.datatorrent.api.AutoMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyMetricsAggregator implements AutoMetric.Aggregator, Serializable
{
  private static final long serialVersionUID = -1790027920120783182L;
  Map<String, Object> result = Maps.newHashMap();

  @Override
  public Map<String, Object> aggregate(long l, Collection<AutoMetric.PhysicalMetricsContext> collection)
  {
    Collection<Collection<Pair<String, Object>>> ret = new ArrayList<>();

    for (AutoMetric.PhysicalMetricsContext pmc : collection) {
      for (Map.Entry<String, Object> metrics : pmc.getMetrics().entrySet()) {
        String key = metrics.getKey();
        Object value = metrics.getValue();
        if (!key.equals("ret")) {
          continue;
        }
        Collection<Collection<Pair<String, Object>>> temp  = (Collection<Collection<Pair<String, Object>>>)value;
        ret.addAll(temp);
      }
    }

    result.put("ret", ret);

    return result;
  }

  private static final Logger LOG = LoggerFactory.getLogger(MyMetricsAggregator.class);
}