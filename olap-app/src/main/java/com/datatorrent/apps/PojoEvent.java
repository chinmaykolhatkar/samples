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

public class PojoEvent
{
  private long time;
  private String name;
  private String dept;
  private long value;

  public long getTime()
  {
    return time;
  }

  public void setTime(long time)
  {
    this.time = time;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public String getDept()
  {
    return dept;
  }

  public void setDept(String dept)
  {
    this.dept = dept;
  }

  public long getValue()
  {
    return value;
  }

  public void setValue(long value)
  {
    this.value = value;
  }
}