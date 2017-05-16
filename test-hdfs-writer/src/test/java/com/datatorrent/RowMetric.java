package com.datatorrent;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by chinmay on 5/5/17.
 */
public class RowMetric implements Serializable
{
  Map<String, Object> data = new HashMap();

  public RowMetric()
  {
    data.put("test1", new Integer(123));
    data.put("test2", "abcdefghij");
    data.put("test3", new Double(123.567));
  }

  private void writeObject(ObjectOutputStream out) throws IOException
  {
    out.writeObject(data.get("test1"));
    out.writeObject(data.get("test2"));
    out.writeObject(data.get("test3"));
  }

  private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException
  {
    if (data == null) {
      data = new HashMap<>();
    }
    data.put("test1", ois.readObject());
    data.put("test2", ois.readObject());
    data.put("test3", ois.readObject());
  }

  public void addFieldValue(String name, Object val)
  {

  }

}
