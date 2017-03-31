package com.datatorrent;

import java.io.Serializable;

/**
 * Created by chinmay on 29/3/17.
 */
public class Record implements Serializable {
  private static final long serialVersionUID = -55857686305273843L;

  private long timestamp;
  private long id;
  private byte[] ranDomArray = new byte[1024*1024];

  public Record(long timestamp, long id) {
    this.timestamp = timestamp;
    this.id = id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public byte[] getRanDomArray() {
    return ranDomArray;
  }

  public void setRanDomArray(byte[] ranDomArray) {
    this.ranDomArray = ranDomArray;
  }

  @Override
  public String toString() {
    return timestamp + "," + id;
  }
}
