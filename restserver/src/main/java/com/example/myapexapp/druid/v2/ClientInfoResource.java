package com.example.myapexapp.druid.v2;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import com.example.myapexapp.RESTManager;

/**
 * Created by chinmay on 7/8/17.
 */
public class ClientInfoResource
{
  public static final String PATH = "druid/v2/datasources";

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public void getDataSources(@Suspended final AsyncResponse response)
  {
    RESTManager.getInstance().registerRequest("", new RESTManager.Responder() {
      @Override
      public void handle(String responseJson)
      {
        try {
          JSONArray jsonArray = new JSONArray(responseJson);
          response.resume(jsonArray);
        } catch (JSONException e) {
          e.printStackTrace();
        }
      }
    });
  }

}
