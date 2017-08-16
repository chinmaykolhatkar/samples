package com.datatorrent.python;

import java.util.Map;

import com.datatorrent.api.Operator;

/**
 * Created by chinmay on 10/8/17.
 */
public interface OperatorExt extends Operator
{
  Map<String, InputPort> getInputPortMap();
  Map<String, OutputPort> getOutputPortMap();
}
