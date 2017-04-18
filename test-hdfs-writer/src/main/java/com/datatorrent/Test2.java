package com.datatorrent;

import java.lang.reflect.Method;

/**
 * Created by chinmay on 13/4/17.
 */
public class Test2 extends Test1
{
  public void write1()
  {
    for (Method method : this.getClass().getDeclaredMethods()) {
      System.out.println(method.getName());
    }

  }
}
