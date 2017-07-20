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
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.olap.EmbeddedDruidOLAPOperator;
import com.datatorrent.olap.query.SampleQueryOperator;

@ApplicationAnnotation(name = "OLAP-App")
public class Application implements StreamingApplication
{
  public void populateDAG(DAG dag, Configuration conf)
  {
    POJOGenerator generator = dag.addOperator("POJOGenerator", POJOGenerator.class);


    EmbeddedDruidOLAPOperator olap = dag.addOperator("OLAP", EmbeddedDruidOLAPOperator.class);
    String s = SchemaUtils.jarResourceFileToString("input.json");
    System.out.println(s);
    olap.setOlapSchema(s);

    SampleQueryOperator query = new SampleQueryOperator();
    olap.setEmbeddableQueryInfoProvider(query);

    ConsoleOutputOperator console = dag.addOperator("Console", ConsoleOutputOperator.class);
    dag.addStream("Stream", generator.out, olap.in);
    dag.addStream("Console", olap.queryResult, console.input);

    dag.setInputPortAttribute(olap.in, Context.PortContext.TUPLE_CLASS, PojoEvent.class);
  }
}
