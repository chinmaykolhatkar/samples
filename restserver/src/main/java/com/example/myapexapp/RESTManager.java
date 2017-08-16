package com.example.myapexapp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

/**
 * Created by chinmay on 3/8/17.
 */
public class RESTManager
{
  private AtomicBoolean setupDone = new AtomicBoolean();
  private AtomicLong requestId = new AtomicLong(1);

  private Server server;
  private int port;
  private BlockingQueue<RequestInfo> requestQueue = new LinkedBlockingQueue<>();

  private static RESTManager manager = new RESTManager();

  public static RESTManager getInstance()
  {
    return manager;
  }


  private int getFreePort() throws IOException
  {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  public void startServlet(String... packages) throws Exception
  {
    if (!setupDone.getAndSet(true)) {
      ServletHolder servlet = createServlet(packages);

      port = getFreePort();
      server = new Server(port);
      ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
      server.setHandler(context);

      context.addServlet(servlet, "/*");
      server.start();
    }
  }

  public String getPort() throws UnknownHostException
  {
    return InetAddress.getLocalHost().getHostAddress() + ":" + port;
  }

  public void stopServlet() throws Exception
  {
    if (setupDone.getAndSet(false)) {
      server.stop();
    }
  }

  public void registerRequest(String request, Responder response)
  {
    long id = requestId.getAndIncrement();
    RequestInfo requestInfo = new RequestInfo(id, request, response);
    requestQueue.offer(requestInfo);
  }

  public BlockingQueue<RequestInfo> getRequestQueue()
  {
    return requestQueue;
  }

  private ServletHolder createServlet(String... packages)
  {
    ResourceConfig rc = new ResourceConfig();
    rc.packages(packages);

    return new ServletHolder(new ServletContainer(rc));
  }

  static class RequestInfo
  {
    public long requestId;
    public String request;
    public Responder response;

    public RequestInfo()
    {
    }

    public RequestInfo(long requestId, String request, Responder response)
    {
      this.requestId = requestId;
      this.request = request;
      this.response = response;
    }
  }

  public interface Responder
  {
    void handle(String response);
  }

}
