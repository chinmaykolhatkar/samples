package com.datatorrent.apps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.common.util.Pair;

public class TopNAggregator implements AutoMetric.Aggregator, Serializable
{
  private static final long serialVersionUID = -1790027920120783452L;
  Map<String, Object> result = Maps.newHashMap();

  @Override
  public Map<String, Object> aggregate(long l, Collection<AutoMetric.PhysicalMetricsContext> collection)
  {
    Collection<Collection<Pair<String, Object>>> ret = new ArrayList<>();

    System.out.println("Size of collection : " + collection.size());

    for (AutoMetric.PhysicalMetricsContext pmc : collection) {
      for (Map.Entry<String, Object> metrics : pmc.getMetrics().entrySet()) {
        String key = metrics.getKey();
        Object value = metrics.getValue();
        if (!key.equals("topN")) {
          continue;
        }
        Collection<Collection<Pair<String, Object>>> temp  = (Collection<Collection<Pair<String, Object>>>)value;
        ret.addAll(temp);
      }
    }

    if (ret.size() > 0) {
      result.put("topN", ret);
    }

    return result;
  }

  private static final Logger LOG = LoggerFactory.getLogger(TopNAggregator.class);
}