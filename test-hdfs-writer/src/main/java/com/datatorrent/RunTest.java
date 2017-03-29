package com.datatorrent;


import java.io.*;
import java.util.Properties;

/**
 * Created by chinmay on 24/3/17.
 */
public class RunTest {

  public static void main(String[] args) throws IOException, InterruptedException, IllegalAccessException, InstantiationException, ClassNotFoundException {
    Properties prop = new Properties();
    InputStream inProp = new FileInputStream(args[0]);
    prop.load(inProp);

    RunTest.print("Starting writer");
    Writer writer = getWriter(prop.getProperty("writerClass"));
    writer.init(prop.getProperty("writePath"));
    WriterThread wt = new WriterThread(writer, Long.parseLong(prop.getProperty
              ("writerTimeoutInterval")));
    wt.start();

    long waitBetweenReaderWriter = Long.parseLong(prop.getProperty("waitBetweenReaderWriter"));
    if (waitBetweenReaderWriter != 0) {
      Thread.sleep(waitBetweenReaderWriter);
    }

    RunTest.print("Starting reader");
    Reader reader = getReader(prop.getProperty("readerClass"));
    reader.init(prop.getProperty("writePath"));
    ReaderThread rt = new ReaderThread(reader, Long.parseLong(prop.getProperty
              ("readerTimeoutInterval")));
    rt.start();

    Thread.sleep(Long.parseLong(prop.getProperty("runInterval")));

    wt.stopThread();
    wt.join();

    rt.stopThread();
    rt.join();

    reader.close();
    writer.close();
  }

  private static Reader getReader(String arg) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    Class<?> aClass = Class.forName(arg);
    if (Reader.class.isAssignableFrom(aClass)) {
      return (Reader) aClass.newInstance();
    }
    else {
      throw new RuntimeException("Reader class incorrect");
    }
  }

  private static Writer getWriter(String arg) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    Class<?> aClass = Class.forName(arg);
    if (Writer.class.isAssignableFrom(aClass)) {
      return (Writer) aClass.newInstance();
    }
    else {
      throw new RuntimeException("Writer class incorrect");
    }
  }

  public static int getSerSize(Object o) throws IOException {
    Record r = new Record(0, 0);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    out.writeObject(r);
    out.flush();
    byte[] bytes = baos.toByteArray();
    return bytes.length;
  }

  public static byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectOutputStream os = new ObjectOutputStream(out);
    os.writeObject(obj);
    out.flush();
    byte[] bytes = out.toByteArray();
    return bytes;
  }

  public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream in = new ObjectInputStream(bais);
    return in.readObject();
  }

  public static class WriterThread extends Thread
  {
    long writeWaitInterval;
    int i = 0;
    boolean stop = false;
    Writer writer;

    public WriterThread(Writer writer, long writeWaitInterval) {
      this.writer = writer;
      this.writeWaitInterval = writeWaitInterval;
    }

    @Override
    public void run() {
      while(!stop) {
        try {
          byte[] s = generateNewData();
          writer.write(s);
          if (writeWaitInterval != 0) {
            Thread.sleep(writeWaitInterval);
          }
        } catch (IOException | InterruptedException e) {
          e.printStackTrace();
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
      }
    }

    private byte[] generateNewData() throws IOException, ClassNotFoundException {
      Record record = new Record(System.currentTimeMillis(), i++);
      byte[] serialize = serialize(record);
      return serialize;
    }

    public void stopThread()
    {
      stop = true;
    }
  }

  public static class ReaderThread extends Thread
  {
    boolean stop = false;
    Reader reader;
    long timeout;
    long objectLength = 0;

    public ReaderThread(Reader reader, long timeout) throws IOException {
      this.reader = reader;
      this.timeout = timeout;
      objectLength = serialize(new Record(0, 0)).length;
    }

    @Override
    public void run() {
      while (!stop) {
        try {
          byte[] read = reader.read(timeout);
          printRecords(read, objectLength);
        } catch (IOException | ClassNotFoundException e) {
          e.printStackTrace();
        }
      }
    }

    private void printRecords(byte[] read, long objectLength) throws IOException, ClassNotFoundException {
      Object deserialize = deserialize(read);
      RunTest.print(System.currentTimeMillis() + "," + deserialize);
    }

    public void stopThread()
    {
      stop = true;
    }
  }

  public static void print(String s)
  {
    System.out.println(System.currentTimeMillis() + " : " + s);
  }
}
