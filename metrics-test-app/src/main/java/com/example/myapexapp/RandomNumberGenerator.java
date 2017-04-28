/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.Pair;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  private int numTuples = 100;
  private transient int count = 0;

  public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();

  @AutoMetric
  private Collection<Collection<Pair<String, Object>>> ret = new ArrayList<>();

  private String operatorId;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    this.operatorId = Integer.toString(context.getId());
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    count = 0;

    ret.clear();
    Collection<Pair<String, Object>> val = new ArrayList<>();
    val.add(new Pair<String, Object>("hashtag", "hello " + operatorId));
    val.add(new Pair<String, Object>("count", new Random().nextInt(100)));

    Collection<Pair<String, Object>> val1 = new ArrayList<>();
    val1.add(new Pair<String, Object>("hashtag", "hello new " + operatorId));
    val1.add(new Pair<String, Object>("count", new Random().nextInt(100)));

    ret.add(val);
    ret.add(val1);
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
      double random = Math.random() * 100;
      out.emit(random);
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   * @param numTuples number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }
}
