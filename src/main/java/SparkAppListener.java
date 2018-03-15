import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import org.apache.spark.launcher.SparkAppHandle;

/**
 * Created by ambarish on 7/3/18.
 */
public class SparkAppListener extends Thread implements SparkAppHandle.Listener
{
  private SparkMonitorApi api;
  private SparkAppHandle sparkAppHandle;
  private String appName;

  public void stateChanged(SparkAppHandle sparkAppHandle)
  {

    this.sparkAppHandle = sparkAppHandle;
  }

  public void infoChanged(SparkAppHandle sparkAppHandle)
  {
    this.sparkAppHandle = sparkAppHandle;
  }

  public SparkAppListener()
  {
    api = new SparkMonitorApi();
    sparkAppHandle = null;
  }

  @Override
  public void run()
  {
    System.out.println("Listener Started");
    Response response;
    String user = "UNKNOWN";
    String duration = "UNKNOWN";
    long startTimeEpoch = 0L;
    long lastUpdatedEpoch = 0L;
    long endTimeEpoch = 0L;
    String timeElapsed = "00:00:00";
    SimpleDateFormat df = new SimpleDateFormat("hh:mm:ss");
    while (true) {
      if (sparkAppHandle != null) {
        try {
          String state = sparkAppHandle.getState().toString();
          String id = sparkAppHandle.getAppId();
          int allJobs=0;
          int activeJobs=0;
          if (sparkAppHandle.getState().equals(SparkAppHandle.State.RUNNING)) {
            response = api.getApplication(id);
            JSONObject applicationDetails = (JSONObject)response.getEntity();
            JSONArray attempts = applicationDetails.getJSONArray("attempts");
            user = attempts.getJSONObject(0).getString("sparkUser");
            startTimeEpoch = attempts.getJSONObject(0).getLong("startTimeEpoch");
            lastUpdatedEpoch = attempts.getJSONObject(0).getLong("lastUpdatedEpoch");
            endTimeEpoch = attempts.getJSONObject(0).getLong("endTimeEpoch");
            timeElapsed = (endTimeEpoch == -1L) ? getElapsedTime(startTimeEpoch, System.currentTimeMillis()) : getElapsedTime
              (startTimeEpoch, endTimeEpoch);
            response = api.getRegisteredMetrics();
            JSONObject metrics = (JSONObject) response.getEntity();
            JSONObject gauges = metrics.getJSONObject("gauges");
            String allJobsProp = id+".driver.DAGScheduler.job.allJobs";
            String activeJobsProp = id+".driver.DAGScheduler.job.activeJobs";
            allJobs = gauges.has(allJobsProp) ? gauges.getJSONObject(allJobsProp).getInt("value") : 0 ;
            activeJobs = gauges.has(activeJobsProp) ? gauges.getJSONObject(activeJobsProp).getInt("value") : 0 ;
          }

          System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
          System.out.println("Application Overview");
          System.out.println("NAME : " + appName);
          System.out.println(String.format("ID : %s | USER : %s | UPTIME : %s", id, user, timeElapsed));
          System.out.println(String.format("STATE : %s | ALL JOBS: %s | ACTIVE JOBS: %s " , state, allJobs, activeJobs
          ));
          System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        } catch (Exception e) {
          System.out.println("Error occurred in request : " + e);
          sparkAppHandle.stop();
          break;
        }

      }else{
        System.out.println("Waiting for App Handle...");

      }
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }
  }

  public void setAppName(String appName)
  {
    this.appName = appName;
  }

  public String getElapsedTime(long start, long end){
    Date d1= new Date(start);
    Date d2 = new Date(end);

    long diff = d2.getTime() - d1.getTime();

    long diffSeconds = diff / 1000 % 60;
    long diffMinutes = diff / (60 * 1000) % 60;
    long diffHours = diff / (60 * 60 * 1000) % 24;

    return String.format("%02d:%02d:%02d", diffHours, diffMinutes, diffSeconds);
  }
}
