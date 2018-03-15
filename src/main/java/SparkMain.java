import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

/**
 * Created by ambarish on 7/3/18.
 */
public class SparkMain
{

  public static void main(String[] args)
  {
    String appName = "WordCountSpark";
    String appPath = "/home/ambarish/tp/scala-trials/target/scala-trials-1.0-SNAPSHOT.jar";
    String mainClass = "com.github.ambarishpande.App";
    String appArgs = "file:///home/ambarish/cat.txt";
    String appArgs2 = "file:///home/ambarish/counts/";
    SparkAppListener listener = new SparkAppListener();
    listener.setAppName(appName);
    listener.start();
    SparkAppHandle sparkLauncher = null;

    try {
      Map<String, String> env= new HashMap<String,String>();
      env.put( "SPARK_HOME", "/usr/local/spark");
      env.put("SPARK_DIST_CLASSPATH", "/usr/local/hadoop/etc/hadoop:/usr/local/hadoop/share/hadoop/common/lib/*:/usr/local/hadoop/share/hadoop/common/*:/usr/local/hadoop/share/hadoop/hdfs:/usr/local/hadoop/share/hadoop/hdfs/lib/*:/usr/local/hadoop/share/hadoop/hdfs/*:/usr/local/hadoop/share/hadoop/yarn/lib/*:/usr/local/hadoop/share/hadoop/yarn/*:/usr/local/hadoop/share/hadoop/mapreduce/lib/*:/usr/local/hadoop/share/hadoop/mapreduce/*:/usr/local/hadoop/contrib/capacity-scheduler/*.jar");
      env.put("SPARK_PRINT_LAUNCH_COMMAND","true");


      //pass in enviroment variables
      sparkLauncher= new SparkLauncher(env)
        //This conf file works well with the spark submit so it shouldn't be source of the issue
        .setPropertiesFile("/usr/local/spark/conf/spark-defaults.conf")
        .setJavaHome("/usr/lib/jvm/java-8-oracle")
//        .setJavaHome("/usr/lib/jvm/java-7-openjdk-amd64")
        .setMainClass(mainClass)
        .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
        .setDeployMode("client")
        .setVerbose(true)
        .setAppName(appName)
        .setAppResource(appPath)
        .addAppArgs(appArgs, appArgs2)
        .redirectError(new File("spark.err"))
        .redirectOutput(new File("spark.out"))
        .startApplication(listener);



//      SparkMonitorApi api = new SparkMonitorApi();
//      Response response;
//      while(true){
//        if(sparkLauncher.getState().equals(SparkAppHandle.State.RUNNING)){
//          try{
//            response = api.getApplication(sparkLauncher.getAppId());
//            JSONObject applicationDetails = (JSONObject) response.getEntity();
//            String name = applicationDetails.getString("name");
//            String id  = applicationDetails.getString("id").split("_")[2];
//            JSONArray attempts = applicationDetails.getJSONArray("attempts");
//            String user = attempts.getJSONObject(0).getString("sparkUser");
//            String state = sparkLauncher.getState().toString();
//
//            String duration = attempts.getJSONObject(0).getString("startTimeEpoch");
//
//            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
//            System.out.println("Application Overview");
//            System.out.println("NAME : " + name);
//            System.out.println(String.format("ID : %s | USER : %s | UPTIME : %s", id, user, duration));
//            System.out.println("STATE : " + state);
//            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
//
//          }catch (Exception e){
//            System.out.println("Error occurred in request : " + e);
//          }
//
//        }
//        Thread.sleep(3000);
//      }

    } catch (IOException e) {
      e.printStackTrace();
    }
    catch(Exception e){
      sparkLauncher.stop();
      System.out.println("General exception");
      e.printStackTrace();
    }
  }
}
