package com.datatorrent;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by chinmay on 24/3/17.
 */
public interface Writer extends Closeable
{
  void write(byte[] byteToWrite) throws IOException;

  void init(String path) throws IOException;
}
