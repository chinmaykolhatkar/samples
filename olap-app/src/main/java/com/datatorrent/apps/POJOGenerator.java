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
  private transient final String[] names = {"Milind", "Tushar", "Bhupesh", "Chinmay", "Mohini", "Sasha"};
  private transient final String[] depts = {"MD", "Backend", "FrontEnd"};

  Random r = new Random();

  private int MAX_EMITS = 10;

  private int emittedRecordCount;

  public final transient DefaultOutputPort<PojoEvent> out = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long windowId)
  {
    emittedRecordCount = 0;
  }

  @Override
  public void emitTuples()
  {
    if (emittedRecordCount++ < MAX_EMITS) {
      out.emit(createRandomPOJO());
    }
  }

  private PojoEvent createRandomPOJO()
  {
    int nameIndex = r.nextInt(names.length) + 1;
    String name = names[nameIndex-1];

    int deptIndex = r.nextInt(depts.length) + 1;
    String dept = depts[deptIndex-1];

    int amount = r.nextInt(2000 - 1000) + 1000;

    PojoEvent e = new PojoEvent();
    e.setTime(System.currentTimeMillis());
    e.setName(name);
    e.setDept(dept);
    e.setValue(amount);
    return e;
  }
}
