package com.datatorrent;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;

import java.io.EOFException;
import java.io.IOException;

/**
 * Created by chinmay on 31/3/17.
 */
public class ReaderImpl3 implements Reader {

  private String filePath;
  private FileSystem fs;
  private HdfsDataInputStream in;
  private int objectLength;
  private long charRead;
  private long startTime;

  @Override
  public byte[] read() throws IOException {
    return read(0);
  }

  @Override
  public byte[] read(long timeout) throws IOException {
    startTime = System.currentTimeMillis();

    while (true) {
      if ((timeout != 0) && (System.currentTimeMillis() - startTime) > timeout) {
        return null;
      }

      byte[] buf = new byte[objectLength];
      try {
        in.readFully(charRead, buf);
        charRead += buf.length;
        return buf;
      } catch (EOFException e) {
        waitFor(10);
        closeFile();
        openFile();
      }
    }
  }

  private void waitFor(long i) {
    try {
      Thread.sleep(i);
    } catch (InterruptedException e) {
      return;
    }
  }

  private void openFile() throws IOException {
    this.in = (HdfsDataInputStream) this.fs.open(new Path(this.filePath));
  }

  private void closeFile() throws IOException {
    this.in.close();
  }

  @Override
  public void init(String path) throws IOException {
    this.objectLength = RunTest.getSerSize(new Record(0, 0));
    this.filePath = path;
    this.fs = FileSystem.get(RunTest.conf);
    openFile();
  }

  @Override
  public void close() throws IOException {
    closeFile();
    this.fs.close();
  }
}
