package com.datatorrent;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;

import java.io.IOException;

public class ReaderImpl2 implements Reader {
  public static final long REOPEN_WAIT_INTERVAL = 100;
  private String filePath;
  private FileSystem fs;
  private DFSInputStream in;
  private long charRead = 0;
  private long objectLength = 0;

  @Override
  public byte[] read() throws IOException {
    return read(0);
  }

  @Override
  public byte[] read(long timeout) throws IOException {
    int byteBufferLength = (int) objectLength;
    long currentFileLength;
    long startTime = System.currentTimeMillis();
    while (!(((currentFileLength = getCurrentFileLength()) - charRead) > byteBufferLength)) {
      RunTest.print("CurrentFileLength = " + currentFileLength + " CharRead : " + charRead);
      if (hasTimedOut(startTime, timeout)) {
        RunTest.print("Timed out");
        return null;
      }
      RunTest.print("Waiting for interval to open the file " + REOPEN_WAIT_INTERVAL);
//      waitFor(REOPEN_WAIT_INTERVAL);
      RunTest.print("Closing the file");
      closeFile();
      RunTest.print("Opening the file");
      openFile();
    }

    byte[] buf = new byte[byteBufferLength];
    this.in.seek(charRead);
    int bytesRead = this.in.read(buf, 0, buf.length);
    charRead += bytesRead;
    return buf;
  }

  private boolean hasTimedOut(long startTime, long timeout) {
    if (timeout != 0) {
      return ((System.currentTimeMillis() - startTime) > timeout);
    }
    else {
      return false;
    }
  }

  private void openFile() throws IOException {
    this.in = (DFSInputStream) this.fs.open(new Path(this.filePath)).getWrappedStream();
  }

  private void closeFile() throws IOException {
    this.in.close();
  }

  private long getCurrentFileLength() {
    return this.in.getFileLength();
  }

  @Override
  public void init(String path) throws IOException {
    Record record = new Record(0, 0);
    this.objectLength = RunTest.getSerSize(record);

    this.filePath = path;
    this.fs = FileSystem.get(RunTest.conf);
    openFile();
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
    closeFile();
    this.fs.close();
  }


}
