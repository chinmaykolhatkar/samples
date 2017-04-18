package com.datatorrent;

import java.lang.reflect.Method;

/**
 * Created by chinmay on 13/4/17.
 */
public class Test1
{
  public void write()
  {
    for (Method method : this.getClass().getDeclaredMethods()) {
      System.out.println(method.getName());
    }

  }
}
