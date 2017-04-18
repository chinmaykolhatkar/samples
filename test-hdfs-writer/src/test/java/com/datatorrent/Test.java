package com.datatorrent;

import java.lang.reflect.Method;

/**
 * Created by chinmay on 13/4/17.
 */
public class Test
{
  public static void main(String[] arg){
    Test2 t = new Test2();
    t.write();
    System.out.println("sdfsdf");
    t.write1();

    System.out.println("sdfsdf");
    Test1 t1 = new Test1();
    t1.write();

    System.out.println("fghljhgjhhjgk");

    for (Method method : Test1.class.getDeclaredMethods()) {
      System.out.println(method.getName());
    }

  }
}
