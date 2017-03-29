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
  FSDataOutputStream out;
  int count = 0;
  public static final int SYNC_AFTER_RECORDS = 1000;

  public void write(byte[] byteToWrite) throws IOException
  {
    ((DFSOutputStream)out.getWrappedStream()).setDropBehind();
    out.write(byteToWrite);
    if (count++ % SYNC_AFTER_RECORDS == 0) {
      ((DFSOutputStream)out.getWrappedStream()).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
    }
    else {
      out.hflush();
    }
  }

  public void init(String path) throws IOException {
    Configuration conf = new Configuration();
    fs = FileSystem.get(conf);

    out = fs.create(new Path(path));
    out.
  }

  public void close() throws IOException
  {
    out.close();
    fs.close();
  }
}
