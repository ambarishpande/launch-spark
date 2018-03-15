import java.io.File;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Created by ambarish on 8/3/18.
 */
public class SparkMonitorApi
{
  public static final String BASE_URI = "http://localhost:4040";

  /**
   * APP STATS
   */
  public static final String API_V1 = "/api/v1";
  public static final String BASE_URI_API = BASE_URI + API_V1;

  public static final String APPLICATIONS = "/applications";
  public static final String JOBS = "/jobs";
  public static final String STAGES = "/stages";
  public static final String TASK_SUMMARY = "/taskSummary";
  public static final String TASK_LIST = "/taskList";
  public static final String RDD = "/storage/rdd";
  public static final String EXECUTORS = "/executors";
  public static final String ALL_EXECUTORS = "/allexecutors";
  public static final String LOGS = "/logs";

  /**
   * METRICS
   */

  public static final String METRICS = "/metrics/json";
  public static final String METRICS_MASTER = "/metrics/master/json";
  public static final String METRICS_APPLICATIONS = "/metrics/applications/json";

  private Client client;

  public SparkMonitorApi()
  {
    this.client = ClientBuilder.newClient();;
  }

  public Response getApplications(){
    return sendRequest(BASE_URI_API + APPLICATIONS);
  }

  public Response getApplication(String appId){
    return sendRequest(BASE_URI_API + APPLICATIONS + File.separator + appId);
  }

  public Response getJobs(String appId){
    return sendRequest(BASE_URI_API + APPLICATIONS + File.separator + appId + JOBS);
  }

  public Response getJob(String appId, String jobId){
    return sendRequest(BASE_URI_API + APPLICATIONS + File.separator + appId + JOBS + File.separator + jobId);
  }

  public Response getStages(String appId){
    return sendRequest(BASE_URI_API + APPLICATIONS + File.separator + appId + STAGES);
  }

  public Response getStage(String appId, String stageId){
    return sendRequest(BASE_URI_API + APPLICATIONS + File.separator + appId + STAGES + File.separator + stageId);
  }

  public Response getStageAttempt(String appId, String stageId, String stageAttemptId){
    return sendRequest(BASE_URI_API + APPLICATIONS + File.separator + appId + STAGES + File.separator + stageId +
      File.separator + stageAttemptId);
  }


  public Response getRegisteredMetrics(){
    return sendRequest(BASE_URI + METRICS);
  }


  public Response getMasterMetrics(){
    return sendRequest(BASE_URI + METRICS_MASTER);
  }

  public Response getApplicationsMetrics(){
    return sendRequest(BASE_URI + METRICS_APPLICATIONS);
  }

  public Response sendRequest(String url){
    System.out.println("GET " + url);
    WebTarget target = client.target(url);
    String responseString;
    Response response = null;
    try {
      responseString =target.request()
        .accept(MediaType.APPLICATION_JSON)
        .get(String.class);

      response = Response.ok().entity(new JSONObject(responseString)).build();
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return response;
  }

}
