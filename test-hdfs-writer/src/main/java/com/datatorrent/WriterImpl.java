package com.datatorrent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.EnumSet;

/**
 * Created by chinmay on 24/3/17.
 */
public class WriterImpl implements Writer
{
  FileSystem fs;
  DFSOutputStream out;
  String path;

  public void write(byte[] byteToWrite) throws IOException
  {
    try {
      RunTest.print("Writing... " + RunTest.deserialize(byteToWrite).toString());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    try {
      out.write(byteToWrite);
      RunTest.print("Hflusing the data...");
      out.hflush();
    } catch (Exception e) {
      RunTest.print("Current Thread is : " + Thread.currentThread().getId());
      e.printStackTrace();
      return;
    }
  }

  public void init(String path) throws IOException {
    this.path = path;
    fs = FileSystem.get(RunTest.conf);

    out = (DFSOutputStream) fs.create(new Path(path)).getWrappedStream();
  }

  public void close() throws IOException
  {
    out.close();
    fs.close();
  }
}
