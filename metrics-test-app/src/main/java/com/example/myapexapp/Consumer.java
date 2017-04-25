package com.example.myapexapp;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.Pair;

/**
 * Created by chinmay on 19/4/17.
 */
public class Consumer extends BaseOperator
{
  @AutoMetric
  private int avg;

  @AutoMetric
  private double sum;

  private int count;

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
    sum = 0;
  }

  public final transient DefaultInputPort<Double> in = new DefaultInputPort<Double>()
  {
    @Override
    public void process(Double tuple)
    {
      count++;
      sum += tuple;
    }
  };

  @Override
  public void endWindow()
  {
    avg = (int)(sum / count);
  }
}
