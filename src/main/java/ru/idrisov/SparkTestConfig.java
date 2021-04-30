package ru.idrisov;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootConfiguration
@ComponentScan
public class SparkTestConfig {

    @Bean
    public SparkSession sparkSession() {
        return SparkSession
                .builder()
                .appName("Spark Test App")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("hive.exec.dynamic.partition", true)
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("spark.sql.crossJoin.enabled", true)
                .getOrCreate();
    }
}
