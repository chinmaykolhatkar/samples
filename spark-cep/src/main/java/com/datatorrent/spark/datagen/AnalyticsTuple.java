/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package com.datatorrent.spark.datagen;

import java.io.Serializable;

/**
 * This is the tuples that will be sent from AnalyticsOutputOperator to Kafka for further OLAP processing.
 */
public class AnalyticsTuple implements Serializable
{
  // Kafka data topic
  public String dsName;
  // Schema if Meta topic; else Data
  public String message;
  // indicates whether this is a schema tuple
  public TupleType tupleType;

  public AnalyticsTuple()
  {
  }

  public AnalyticsTuple(String dsName, String message, TupleType tupleType)
  {
    this.dsName = dsName;
    this.message = message;
    this.tupleType = tupleType;
  }

  public String getDsName()
  {
    return dsName;
  }

  public void setDsName(String dsName)
  {
    this.dsName = dsName;
  }

  public String getMessage()
  {
    return message;
  }

  public void setMessage(String message)
  {
    this.message = message;
  }

  public TupleType getTupleType()
  {
    return tupleType;
  }

  public void setTupleType(TupleType tupleType)
  {
    this.tupleType = tupleType;
  }

  @Override
  public String toString()
  {
    return "AnalyticsTuple{" +
      "dsName='" + dsName + '\'' +
      ", message='" + message + '\'' +
      ", tupleType=" + tupleType +
      '}';
  }

  public enum TupleType
  {
    ADD_DATA_SOURCE,
    DELETE_DATA_SOURCE,
    DATA
  }
}
