package com.datatorrent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Created by chinmay on 13/4/17.
 */
public class Test
{
  public static void main(String[] arg) throws IOException, ClassNotFoundException, JSONException
  {
    String json = "{\"opName\":{\"abc\":\"int\", \"pqr\": \"float\"}}";
    JSONObject o = new JSONObject(json);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(o);

    baos.flush();

    byte[] bytes = baos.toByteArray();

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    Object o1 = ois.readObject();
    System.out.println(o1);
  }
}
