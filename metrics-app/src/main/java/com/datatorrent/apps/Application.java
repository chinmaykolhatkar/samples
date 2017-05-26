/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datatorrent.apps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.filter.FilterOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.metrics.appmetrics.AppMetricComputeService;

@ApplicationAnnotation(name = "Metric-App")
public class Application implements StreamingApplication
{
  public void populateDAG(DAG dag, Configuration conf)
  {
    POJOGenerator generator = dag.addOperator("POJOGenerator", POJOGenerator.class);
    CsvParser csvParser = dag.addOperator("csvParser", CsvParser.class);
    FilterOperator filterOperator = dag.addOperator("filter", new FilterOperator());
    CsvFormatter formatter = dag.addOperator("formatter", new CsvFormatter());
    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());

    dag.addStream("data", generator.out, csvParser.in);
    dag.addStream("pojo", csvParser.out, filterOperator.input);
    dag.addStream("filtered", filterOperator.truePort, formatter.in);
    dag.addStream("string", formatter.out, console.input);
    
    dag.setAttribute(AppMetricComputeService.APP_METRIC_COMPUTE_SERVICE, new AppMetricsService());
    dag.setAttribute(Context.DAGContext.METRICS_TRANSPORT, null);
  }
}
