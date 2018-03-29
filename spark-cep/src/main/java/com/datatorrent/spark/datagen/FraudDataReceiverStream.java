package com.datatorrent.spark.datagen;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * Created by chinmay on 24/3/18.
 */
public class FraudDataReceiverStream extends Receiver<FraudPojo>
{
  private int ratePerSec;
  // Config
  public double fanout = 0.2;
  public int numDimensions;
  public int numMeasures;
  public int[] cardinalities;
  public String[] dimensions;
  public String[] measures;
  public String baseDataPath;
  public long numTuplesToGenerate = 10000;

  // Internal
  private Map<String, List<String>> masterData;
  private int minCardinality;
  private int maxCardinality;
  private Random r = new Random();
  private long startTS = System.currentTimeMillis() - 30000; // currentTime - 5min

  public FraudDataReceiverStream(int ratePerSec) throws NoSuchFieldException
  {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.ratePerSec = ratePerSec;
    numDimensions = 5;
    numMeasures = 1;
    dimensions = new String[] {"email", "country", "cctype", "currency", "isfraud"}; // ccnumber, currency type etc..

    cardinalities = new int[] {20000, 127, 16, 59, 2};
    measures = new String[] {"amount"};
    baseDataPath = "src/main/resources/";

    // upload masterdata
    masterData = new HashMap<>();
    for (int i = 0; i < dimensions.length; i++) {
      try {
        masterData.put(dimensions[i], readFile(dimensions[i]));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public List<String> readFile(String path) throws IOException
  {
    List<String> retVal = new ArrayList<>();
    InputStream in = FraudDataReceiverStream.class.getResourceAsStream("/" + path);
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String s;
    while ((s = br.readLine()) != null) {
      retVal.add(s);
    }
    return retVal;
  }

  public void init()
  {
    if (cardinalities.length != numDimensions) {
      throw new IllegalArgumentException("Cardinalities not specified for all dimensions");
    }
    minCardinality = NumberUtils.min(cardinalities);
    maxCardinality = NumberUtils.max(cardinalities);
    if (minCardinality == maxCardinality) {
      maxCardinality++;
    }
  }

  @Override
  public void onStart()
  {
    new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        produceData();
      }
    }).start();
  }

  public FraudPojo gen(Field[] setters) throws IllegalAccessException
  {
    FraudPojo pojo = new FraudPojo();
    pojo.dataSource = "fraud";
    int lastCardinality = maxCardinality;
    int lastVal = ThreadLocalRandom.current().nextInt(minCardinality, maxCardinality);
    StringBuffer record = new StringBuffer();
    startTS += r.nextInt(10000);
    record.append("{ \"timestamp\" : \"" + startTS + "\", ");
    pojo.timestamp = System.currentTimeMillis();
    for (int c = 0; c < numDimensions; c++) {
      lastVal = vary(project(lastVal, lastCardinality, cardinalities[c]), cardinalities[c]);
      record.append("\"" + dimensions[c] + "\" : \"" + masterData.get(dimensions[c]).get(lastVal) + "\", ");
      setters[c].set(pojo, masterData.get(dimensions[c]).get(lastVal));
      lastCardinality = cardinalities[c];
    }
    for (int m = 0; m < numMeasures; m++) {
      long num = r.nextInt(100000000);
      record.append("\"" + measures[m] + "\" : \"" + num + "\", ");
      int setterIndex = numDimensions + m;
      setters[setterIndex].set(pojo, num);
    }
    return pojo;
  }

  private Field[] getSetters(Class fraudPojoClass) throws NoSuchFieldException
  {
    Field[] setters = new Field[] {FraudPojo.class.getField("email"),
      FraudPojo.class.getField("country"),
      FraudPojo.class.getField("cctype"),
      FraudPojo.class.getField("currency"),
      FraudPojo.class.getField("isfraud"),
      FraudPojo.class.getField("amount")
    }; // ccnumber, currency type etc..

    for (int i=0; i < setters.length; ++i) {
      setters[i].setAccessible(true);
    }

    return setters;
  }

  private int vary(int n, int cardinality)
  {
    int variance = (int)(cardinality * fanout / 2);
    int lower = n - variance;
    int upper = n + variance;
    int random = r.nextInt((upper - lower) + 1) + lower;
    int retVal = mod(random, cardinality);
    return retVal;
  }

  private int mod(int a, int b)
  {
    int r = a % b;
    return r < 0 ? r + b : r;
  }

  private int project(int n, int fromCard, int toCard)
  {
    return n * toCard / fromCard;
  }

  private void produceData()
  {
    init();
    Field[] setters;
    try {
      setters = getSetters(FraudPojo.class);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }

    while (true) {
      try {
        FraudPojo gen = gen(setters);
        store(gen);
        Thread.sleep((long)(1000 / ratePerSec));
      } catch (IllegalAccessException | InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void onStop()
  {
    // Do nothing
  }
}
