/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.example.myapexapp.Application;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  @Test
  public void anotherTest()
  {
    long EOR_MARKER = 0x00000000ffffffffL;
    long recordLength = 56;
    System.out.println(recordLength);

    long EOR = (recordLength << 32) | EOR_MARKER;
    System.out.println(EOR);

    if ((EOR & EOR_MARKER) != EOR_MARKER) {
      throw new RuntimeException("Failed to validate EOR marker.");
    }

    long orig = EOR >> 32;
    System.out.println(orig);
  }
}
