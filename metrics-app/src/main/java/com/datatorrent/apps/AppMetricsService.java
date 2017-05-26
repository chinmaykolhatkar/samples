package com.datatorrent.apps;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.metrics.appmetrics.AppMetricComputeService.DefaultAppMetricComputeService;

public class AppMetricsService extends DefaultAppMetricComputeService
{
  private final Logger LOG = LoggerFactory.getLogger(AppMetricsService.class);
  @Override
  public Map<String, Object> computeAppLevelMetrics(Map<String, Map<String, Object>> completedMetrics)
  {
    LOG.info("completedMetrics {}", completedMetrics);
    Long incoming = (Long) completedMetrics.get("csvParser").get("incomingTuplesCount");
    Long filtered = (Long) completedMetrics.get("filter").get("trueTuples");
    LOG.info("incoming {}, filtered{}", incoming, filtered);

    Map<String, Object> output = Maps.newHashMap();
    if(incoming != null && filtered != null){
      if(incoming != 0){
        double percentFiltered = (filtered * 100.0) /incoming;
        output.put("percentFiltered", percentFiltered);
        LOG.info("percentFiltered {}", percentFiltered);
      }
    }
    return output;
  }


  private static final long serialVersionUID = 5119330693347067792L;


}
