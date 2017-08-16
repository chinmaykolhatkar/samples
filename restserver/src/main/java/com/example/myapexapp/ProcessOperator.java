package com.example.myapexapp;

import java.io.IOException;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.apache.directory.api.util.Strings;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;

/**
 * Created by chinmay on 3/8/17.
 */
public class ProcessOperator extends BaseOperator
{
  public transient final DefaultInputPort<KeyValPair<Long, String>> in = new DefaultInputPort<KeyValPair<Long, String>>()
  {
    @Override
    public void process(KeyValPair<Long, String> tuple)
    {
      processRequest(tuple);
    }
  };

  public transient final DefaultOutputPort<KeyValPair<Long, String>> out = new DefaultOutputPort<>();

  private void processRequest(KeyValPair<Long, String> tuple)
  {
    try {
      if (Strings.isEmpty(tuple.getValue())) {
        JSONArray a = new JSONArray();
        a.put("test1");
        a.put("test2");
        out.emit(new KeyValPair<>(tuple.getKey(), a.toString()));
      } else {
        DefaultObjectMapper mapper = new DefaultObjectMapper();
        try {
          Query query = mapper.readValue(tuple.getValue(), Query.class);
          out.emit(new KeyValPair<>(tuple.getKey(), mapper.writeValueAsString(query)));
        } catch (IOException e) {
          JSONObject jsonObject = new JSONObject(tuple.getValue());
          int no = jsonObject.getInt("no");
          jsonObject.put("no", no * 2);
          out.emit(new KeyValPair<>(tuple.getKey(), jsonObject.toString()));
        }

      }
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }
}
