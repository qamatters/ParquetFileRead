import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.testng.annotations.Test;

import static com.SparkHelper.*;

public class sparkSQL {
    @Test
    public static  void query() throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();
        runBasicDataFrameExample(spark);
        spark.stop();
    }

}
