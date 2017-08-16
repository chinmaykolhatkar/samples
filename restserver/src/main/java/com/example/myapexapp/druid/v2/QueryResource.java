package com.example.myapexapp.druid.v2;

import java.io.IOException;
import java.io.InputStream;

import javax.management.Query;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import com.example.myapexapp.RESTManager;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.druid.jackson.DefaultObjectMapper;

/**
 * Created by chinmay on 7/8/17.
 */

public class QueryResource
{
  public static final String PATH = "druid/v2";

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public void doQuery(
    @Suspended final AsyncResponse response,
    InputStream in
  )
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    Query query;
    String queryStr = null;
    try {
      query = objectMapper.readValue(in, Query.class);
      queryStr = objectMapper.writeValueAsString(query);
    } catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println("query : " + queryStr);

    RESTManager.getInstance().registerRequest(queryStr, new RESTManager.Responder()
    {
      @Override
      public void handle(String responseStr)
      {
        response.resume(responseStr);
      }
    });


  }

}
