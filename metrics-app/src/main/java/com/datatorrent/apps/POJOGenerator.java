package com.datatorrent.apps;

import java.util.Random;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by chinmay on 26/5/17.
 */
public class POJOGenerator extends BaseOperator implements InputOperator
{
  private transient final String[] names = {"Milind", "Sandeep", "Tushar", "Chaitanya", "Anirudha", "Yogi", "Priyanka", "Pradeep K", "Bhupesh", "Chinmay", "Shubham", "Mohit", "Mohini", "Hitesh", "Deepak", "Francis", "Bhakti", "Ajay", "Vikram", "Ankit", "Yatin", "NIket", "Gurudas", "Aishwarya", "Shubham", "Plaban", "Ameya", "Aditya", "Pooja"};

  Random r = new Random();

  private int MAX_EMITS = 1000;

  @AutoMetric
  private int emittedRecordCount;

  @AutoMetric
  private int windowedRecordSize;


  public final transient DefaultOutputPort<byte[]> out = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long windowId)
  {
    emittedRecordCount = 0;
    windowedRecordSize = 0;
  }

  @Override
  public void emitTuples()
  {
    if (emittedRecordCount++ < MAX_EMITS) {
      byte[] randomPOJO = createRandomPOJO();
      windowedRecordSize += randomPOJO.length;
      out.emit(randomPOJO);
    }
  }

  private byte[] createRandomPOJO()
  {
    int accNo = r.nextInt(names.length) + 1;
    String name = names[accNo-1];
    int amount = r.nextInt(20500 - 19500) + 19500;

    return (accNo + "|" + name + "|" + amount).getBytes();
  }
}
