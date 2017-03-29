package com.example.myapexapp;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by chinmay on 8/3/17.
 */
public class TestOperator1 extends BaseOperator
{
    @InputPortFieldAnnotation(optional = true)
    public final transient DefaultInputPort<Double> in1 = new DefaultInputPort<Double>() {
        @Override
        public void process(Double tuple) {

        }
    };

    @InputPortFieldAnnotation(optional = true)
    public final transient DefaultInputPort<Double> in2 = new DefaultInputPort<Double>() {
        @Override
        public void process(Double tuple) {

        }
    };

}
