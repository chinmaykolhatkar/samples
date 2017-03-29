package com.datatorrent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;

import java.io.EOFException;
import java.io.IOException;

public class ReaderImpl2 implements Reader {
  public static final long REOPEN_WAIT_INTERVAL = 500;
  private String filePath;
  private FileSystem fs;
  private HdfsDataInputStream in;
  private long visibleLength = 0;
  private long charRead = 0;
  private long objectLength = 0;

  @Override
  public byte[] read() throws IOException {
    return read(0);
  }

  @Override
  public byte[] read(long timeout) throws IOException {
    reopen();
//    int byteBufferLength = (int) ((((visibleLength - charRead) / objectLength) - 1) * objectLength);
    int byteBufferLength = (int) objectLength;
    byte[] buf = new byte[byteBufferLength];
    try {
      in.readFully(charRead, buf);
    } catch (EOFException e) {
      read(0);
    }
    charRead += buf.length;
    return buf;
  }

  @Override
  public void init(String path) throws IOException {
    Record record = new Record(0, 0);
    byte[] serialize = RunTest.serialize(record);
    this.objectLength = serialize.length;

    this.filePath = path;
    this.fs = FileSystem.get(new Configuration());
    this.in = (HdfsDataInputStream) this.fs.open(new Path(this.filePath));
    reopen();
  }

  public void reopen() throws IOException {
    long length;
    while ((length = in.getVisibleLength()) <= visibleLength) {
      in.close();
      System.out.println("At " + System.currentTimeMillis() + ", waiting for reopen..");
      waitFor(REOPEN_WAIT_INTERVAL);
      in = (HdfsDataInputStream) this.fs.open(new Path(this.filePath));
    }
    this.visibleLength = length;
  }

  void waitFor(long interval)
  {
    try {
      Thread.sleep(interval);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
