package com.datatorrent;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by chinmay on 24/3/17.
 */
public interface Reader extends Closeable
{
  byte[] read() throws IOException;

  byte[] read(long timeout) throws IOException;

  void init(String path) throws IOException;
}
