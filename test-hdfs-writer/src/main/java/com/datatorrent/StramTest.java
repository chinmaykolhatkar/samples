package com.datatorrent;

import java.io.IOException;

import org.codehaus.jettison.json.JSONObject;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.metrics.consumer.QueryFetcher;
import com.datatorrent.metrics.store.impl.HDFSStoreConf;

/**
 * Created by chinmay on 24/4/17.
 */
public class StramTest
{
  public static void main(String[] arg) throws IOException
  {
    Configuration conf = new Configuration();
    String s = conf.get(HDFSStoreConf.CONF_METRICS_DIR);
    conf.set(HDFSStoreConf.CONF_METRICS_DIR, "datatorrent/metrics/" + arg[0] + "/" + arg[1]);

    QueryFetcher q = new QueryFetcher(conf);
    q.toLastLogicalAutoMetrics();
    while (true) {
      try {
        JSONObject metricsInformation = q.getLogicalAutoMetrics(arg[2]);
        if (metricsInformation == null) {
          System.out.println("No metrics found");
        } else {
          System.out.println("Metric found : " + metricsInformation.toString());
        }
        Thread.sleep(500);
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }
}
