package com.example.myapexapp;

import org.codehaus.jettison.json.JSONArray;

/**
 * Created by chinmay on 18/5/17.
 */
public class Test
{
  @org.junit.Test
  public void test()
  {
    JSONArray arr1 = new JSONArray();
    arr1.put("String1");

    JSONArray arr2 = new JSONArray();
    arr2.put("String2");

    arr1.put(arr2);

    System.out.println(arr1);

  }
}
