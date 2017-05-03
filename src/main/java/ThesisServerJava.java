import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;

/**
 * Created by ppetrou on 4/29/17.
 */
public class ThesisServerJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                //.enableHiveSupport()
                .master("local[*]") //TODO delete in cluster mode
                .getOrCreate();

        //partition

        HiveThriftServer2.startWithContext(spark.sqlContext());

    }
}