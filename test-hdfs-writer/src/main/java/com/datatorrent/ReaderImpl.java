package com.datatorrent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by chinmay on 24/3/17.
 */
public class ReaderImpl implements Reader {
  protected String path;
  protected FileSystem fs;
  protected BufferedReader br;

  protected int charRead = 0;
  public static long INITWAIT = 100;
  public static long MAXWAIT = 100;
  private long currentWait;

  public byte[] read() throws IOException {
    currentWait = INITWAIT;
    String data;
    while ((data = br.readLine()) == null) {
      br.close();
      waitForInterval();
      reopen();
    }
    charRead += data.length() + 1;
    return data.getBytes();
  }

  @Override
  public byte[] read(long timeout) throws IOException {
    currentWait = INITWAIT;
    long startTime = System.currentTimeMillis();
    String data;
    while ((data = br.readLine()) == null) {
      if ((System.currentTimeMillis() - startTime) > timeout) {
        return null;
      }
      br.close();
      waitForInterval();
      reopen();
    }
    charRead += data.length() + 1;
    return data.getBytes();
  }

  public void init(String path) throws IOException {
    this.path = path;
    fs = FileSystem.get(RunTest.conf);
    br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
  }

  public void close() throws IOException {
    br.close();
    fs.close();
  }

  private void waitForInterval() {
    currentWait = Math.min(2 * currentWait, MAXWAIT);

    try {
      Thread.sleep(currentWait);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void reopen() throws IOException {
    br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
    br.skip(charRead);
  }
}
