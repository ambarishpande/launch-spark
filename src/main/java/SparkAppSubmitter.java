import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

/**
 * Created by ambarish on 7/3/18.
 */
public class SparkAppSubmitter
{
  public static final String JAR = "--jar";
  public static final String CLASS = "--class";
  public static final String ARG = "--arg";

  public ApplicationId submitToYarn(String appPath, String applicationName, String mainClass, String[] args) throws
    Exception {

    LinkedList<String> argsList = new LinkedList<String>();
    argsList.add(JAR);
    argsList.add(appPath);
    argsList.add(CLASS);
    argsList.add(mainClass);

    for (String arg : args ) {
      argsList.add(ARG);
      argsList.add(arg);
    }

    // Hadoop Conf
    Configuration config = new Configuration();

    // Spark Conf
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster("yarn");
    sparkConf.setAppName(applicationName);
    sparkConf.set("master", "yarn");
    sparkConf.set("spark.submit.deployMode", "cluster"); // worked

    ClientArguments clientArguments = new ClientArguments((String[])argsList.toArray());  // spark-2.0.0
    Client client = new Client(clientArguments, config, sparkConf);
    return client.submitApplication();

  }

}
