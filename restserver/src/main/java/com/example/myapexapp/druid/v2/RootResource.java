package com.example.myapexapp.druid.v2;

import javax.ws.rs.Path;
import javax.ws.rs.core.Application;

/**
 * Created by chinmay on 7/8/17.
 */
@Path(RootResource.PATH)
public class RootResource extends Application
{
  public static final String PATH="/";

  @Path(QueryResource.PATH)
  public QueryResource getQueryResource()
  {
    return new QueryResource();
  }

  @Path(HelloWorldResources.PATH)
  public HelloWorldResources getHelloWorldResource()
  {
    return new HelloWorldResources();
  }

  @Path(ClientInfoResource.PATH)
  public ClientInfoResource getClientInfoResource()
  {
    return new ClientInfoResource();
  }
}
