import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ambarish on 8/3/18.
 */
public class SparkMonitorApiTest
{
  SparkMonitorApi api;

  @Before
  public void setup(){
    api = new SparkMonitorApi();
  }
  @Test
  public void testSendRequest() throws JSONException
  {
    String url = "http://localhost:9090/ws/v2/about";
    Response response = api.sendRequest(url);
    JSONObject jsonObject = (JSONObject) response.getEntity();
    System.out.println(jsonObject.toString(4));
  }

  @Test
  public void testDateDiff(){
    long l = 1520579688668L;
    long l2 = 1520589688668L;

    SparkAppListener listener = new SparkAppListener();
    System.out.println(listener.getElapsedTime(l,l2));
  }


}