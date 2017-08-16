package com.datatorrent.python;

/**
 * Created by chinmay on 10/8/17.
 */
public interface PythonPort
{
  interface InputPort
  {
    void process(Object o);
  }

  interface OutputPort
  {
    void emit(Object o);
  }
}
