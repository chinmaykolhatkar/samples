package com.example.myapexapp;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by chinmay on 19/4/17.
 */
public class Consumer extends BaseOperator
{
  @AutoMetric
  private double avg;

  @AutoMetric
  private double sum;

  @AutoMetric
  private int c;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    c = 0;
    sum = 0;
  }

  public final transient DefaultInputPort<Double> in = new DefaultInputPort<Double>()
  {
    @Override
    public void process(Double tuple)
    {
      c++;
      sum += tuple;
    }
  };

  @Override
  public void endWindow()
  {
    avg = sum / c;
  }
}
