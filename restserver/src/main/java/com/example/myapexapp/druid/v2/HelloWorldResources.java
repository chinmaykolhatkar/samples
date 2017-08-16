package com.example.myapexapp.druid.v2;

import java.io.InputStream;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import com.example.myapexapp.RESTManager;
import com.example.myapexapp.RESTManager.Responder;

/**
 * Created by chinmay on 3/8/17.
 */
public class HelloWorldResources
{
  public static final String PATH = "hello";

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public void helloWorld(@Suspended final AsyncResponse response,
    @QueryParam("request") String requestJson) {
    RESTManager.getInstance().registerRequest(requestJson, new Responder(){
      @Override
      public void handle(String responseJson)
      {
        response.resume(responseJson);
      }
    });
  }

  @POST
  @Path("")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public void doPost(
    @Suspended final AsyncResponse response,
    InputStream in,
    @QueryParam("pretty") String pretty
  )
  {

  }

}
