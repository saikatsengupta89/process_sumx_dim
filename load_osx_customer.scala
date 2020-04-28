package load_dimension
import java.sql.DriverManager
import java.util.Properties

import org.apache.log4j._
import org.apache.spark.SparkConf;
import org.apache.spark._  
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{StringType ,IntegerType}

import java.net.Authenticator
import org.apache.hadoop.conf.Configuration

/* for writing logs w.r.t. data load process */
import java.io._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.YearMonth


object load_osx_customer {
  def time:String={
    
    /* GET CURRENT DATESTAMP FOR LOG WRITING TO YARN */
    val timeformat= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    return timeformat.format(new Date()).toString()
  }
  
  def sparkConfig():SparkConf = {
    
    val conf = new SparkConf().
                            setMaster("yarn").
                            setAppName("ProcessSumx - DimensionLoad").
                            set("spark.hadoop.fs.defaultFS","hdfs://dev1node01.fgb.ae:8020").
                            set("spark.hadoop.yarn.resourcemanager.address", "dev1node04.fgb.ae:8032").
                            set("spark.hadoop.yarn.resourcemanager.hostname","dev1node04.fgb.ae").
//                            set("spark.yarn.jars","hdfs://dev1node01.fgb.ae:8020/user/spark/share/lib/*.jar").
//                            set("spark.hadoop.yarn.application.classpath", 
//                                "$HADOOP_CLIENT_CONF_DIR,"
//                               +"$HADOOP_CONF_DIR,"
//                               +"$HADOOP_COMMON_HOME/*,"
//                               +"$HADOOP_COMMON_HOME/lib/*,"
//                               +"$HADOOP_HDFS_HOME/*,"
//                               +"$HADOOP_HDFS_HOME/lib/*,"
//                               +"$HADOOP_YARN_HOME/*,"
//                               +"$HADOOP_YARN_HOME/lib/*").
//                            set("spark.sql.warehouse.dir","hdfs://bda1node01.fgb.ae:8020/user/hive/warehouse").
//                            set("hive.metastore.uris","thrift://bda1node01.fgb.ae:9083").
//                            set("spark.local.dir","/tmp/spark_temp").
                            set("spark.hadoop.validateOutputSpecs", "false").
                            set("spark.sql.codegen.wholeStage","false"). // TO STOP CODEGEN TO GENERATE PARQUET STRING WHICH THROWS ERROR
                            set("spark.hadoop.hadoop.security.authentication", "kerberos").
                            set("spark.hadoop.hadoop.security.authorization", "true").
                            set("spark.hadoop.dfs.namenode.kerberos.principal","hdfs/dev1node01.fgb.ae@FGB.AE").
                            set("spark.hadoop.yarn.resourcemanager.principal", "yarn/dev1node01.fgb.ae@FGB.AE").
                            set("spark.yarn.keytab", "/home/o2072/o2072.keytab").
                            set("spark.yarn.principal", "o2072@FGB.AE").
                            set("spark.yarn.access.hadoopFileSystem", "hdfs://CDHCluster-ns:8020").
                            set("spark.yarn.access.namenodes","hdfs://CDHCluster-ns")
      
      println(time+" "+"SPARK CONFIG set")
      return conf
  }
  
  def main (args:Array[String]) {
    
    val sparkConf= sparkConfig()
    val sparkContext= new SparkContext(sparkConf)
    val sqlContext= new SQLContext(sparkContext)
    
    val hdfs_osx_cust_raw= "/raw/onesumx/daily/T_OUTBOUND_CUSTOMER_FGB.DAT"
    val hdfs_osx_customer= "/data/fin_onesumx/dim_osx_customer"
    
    val osx_customer_schema =StructType(Array(
        StructField("customer_nr",StringType, true),
        StructField("start_validity_date",StringType, true),
        StructField("end_validity_date",StringType, true),
        StructField("customer_shortname",StringType, true),
        StructField("cust_name_address_1",StringType, true),
        StructField("cust_name_address_2",StringType, true),
        StructField("cust_name_address_3",StringType, true),
        StructField("cust_name_address_4",StringType, true),
        StructField("is_active",StringType, true),
        StructField("is_parent_ind",StringType, true),
        StructField("account_officer",StringType, true),
        StructField("nationality",StringType, true),
        StructField("domicile",StringType, true),
        StructField("intercompany",StringType, true),
        StructField("credit_worthiness",StringType, true),
        StructField("industry_code",StringType, true),
        StructField("customer_attribute1",StringType, true),
        StructField("customer_attribute2",StringType, true),
        StructField("customer_attribute3",StringType, true),
        StructField("customer_attribute4",StringType, true),
        StructField("customer_attribute5",StringType, true),
        StructField("customer_attribute6",StringType, true),
        StructField("customer_attribute7",StringType, true),
        StructField("customer_attribute8",StringType, true),
        StructField("customer_attribute9",StringType, true),
        StructField("customer_attribute10",StringType, true),
        StructField("customer_attribute11",StringType, true),
        StructField("customer_attribute12",StringType, true),
        StructField("customer_attribute13",StringType, true),
        StructField("customer_attribute14",StringType, true),
        StructField("customer_attribute15",StringType, true),
        StructField("customer_attribute16",StringType, true),
        StructField("customer_attribute17",StringType, true),
        StructField("customer_attribute18",StringType, true),
        StructField("customer_attribute19",StringType, true),
        StructField("customer_attribute20",StringType, true),
        StructField("customer_attribute21",StringType, true),
        StructField("customer_attribute22",StringType, true),
        StructField("customer_attribute23",StringType, true),
        StructField("customer_attribute24",StringType, true),
        StructField("customer_telephone_nr",StringType, true),
        StructField("customer_type",StringType, true),
        StructField("economic_sector",StringType, true),
        StructField("global_customer",StringType, true),
        StructField("nace_code",StringType, true),
        StructField("mis_master",StringType, true),
        StructField("is_repo_core_mkt_participant",StringType, true),
        StructField("lei_code",StringType, true),
        StructField("is_sme",IntegerType, true),
        StructField("is_spe",IntegerType, true),
        StructField("is_student",IntegerType, true),
        StructField("is_rated",IntegerType, true),
        StructField("is_defaulted",IntegerType, true),
        StructField("coverage_team",StringType, true),
        StructField("crd_ind",StringType, true),
        StructField("skyfall_ind",StringType, true),
        StructField("group_uid",StringType, true),
        StructField("group_name",StringType, true),
        StructField("global_rm_code",StringType, true),
        StructField("global_rm_name",StringType, true),
        StructField("local_rm_code",StringType, true),
        StructField("local_rm_name",StringType, true),
        StructField("last_modified",StringType, true),
        StructField("modified_by",StringType, true)
    ))
    
    println(time+" "+"DIM_OSX_CUSTOMER - SCHEMA INITIALIZED")
    
    val osx_customer_base= sqlContext.read.format("com.databricks.spark.csv")
                               .option("header", "true")
                               .option("delimiter","~")
                               .schema(osx_customer_schema)
                               .load (hdfs_osx_cust_raw)
                               
    //val osx_customer_text= sparkContext.textFile(hdfs_osx_customer, 4)
    //val header= osx_customer_text.first()
    //val osx_data= osx_customer_text.filter(x=> x!=header)
    //print(osx_data.count())
    
    println(time+" "+"DIM_OSX_CUSTOMER - LOAD INITIATED")                               
                               
    osx_customer_base.registerTempTable("DIM_OSX_CUSTOMER_INTERIM")
    //osx_customer_base.show(30)
    
    
    val osx_customer_transformed= sqlContext.sql (
        "SELECT "+
        "CUSTOMER_NUMBER, "+
        "CUSTOMER_NAME, "+
        "CUST_NAME_ADDRESS_1, "+
        "CUST_NAME_ADDRESS_2, "+
        "CUST_NAME_ADDRESS_3, "+
        "CUST_NAME_ADDRESS_4, "+
        "CUSTOMER_STATUS, "+
        "IS_PARENT_IND, "+
        "ACCOUNT_OFFICER, "+
        "NATIONALITY, "+
        "DOMICILE, "+
        "INTERCOMPANY, "+
        "CREDIT_WORTHINESS, "+
        "CUSTOMER_SEGMENT_CODE, "+
        "BANKING_TYPE, "+
        "CUSTOMER_ATTRIBUTE1, "+
        "CUSTOMER_ATTRIBUTE2, "+
        "CUSTOMER_ATTRIBUTE3, "+
        "CUSTOMER_ATTRIBUTE4, "+
        "CUSTOMER_ATTRIBUTE5, "+
        "CUSTOMER_ATTRIBUTE6, "+
        "CUSTOMER_ATTRIBUTE7, "+
        "CUSTOMER_ATTRIBUTE8, "+
        "CUSTOMER_ATTRIBUTE9, "+
        "CUSTOMER_ATTRIBUTE10, "+
        "CUSTOMER_ATTRIBUTE11, "+
        "CUSTOMER_ATTRIBUTE12, "+
        "CUSTOMER_ATTRIBUTE13, "+
        "CUSTOMER_ATTRIBUTE14, "+
        "CUSTOMER_ATTRIBUTE15, "+
        "CUSTOMER_ATTRIBUTE16, "+
        "CUSTOMER_ATTRIBUTE17, "+
        "CUSTOMER_ATTRIBUTE18, "+
        "CUSTOMER_ATTRIBUTE19, "+
        "CUSTOMER_ATTRIBUTE20, "+
        "CUSTOMER_ATTRIBUTE21, "+
        "CUSTOMER_ATTRIBUTE22, "+
        "CUSTOMER_TELEPHONE_NR, "+
        "CUSTOMER_ATTRIBUTE23, "+
        "CUSTOMER_ATTRIBUTE24, "+
        "CUSTOMER_TYPE, "+
        "ECONOMIC_SECTOR, "+
        "GLOBAL_CUSTOMER, "+
        "NACE_CODE, "+
        "MIS_MASTER, "+
        "IS_REPO_CORE_MKT_PARTICIPANT, "+
        "START_VALIDITY_DATE, "+
        "END_VALIDITY_DATE, "+
        "LEI_CODE, "+
        "IS_SME, "+
        "IS_SPE, "+
        "IS_STUDENT, "+
        "IS_RATED, "+
        "IS_DEFAULTED, "+
        "COVERAGE_TEAM, "+
        "CRD_IND, "+
        "SKYFALL_IND, "+
        "GROUP_UID, "+
        "GROUP_NAME, "+
        "GLOBAL_RM_CODE, "+
        "GLOBAL_RM_NAME, "+
        "LOCAL_RM_CODE, "+
        "LOCAL_RM_NAME, "+
        "SOURCE_SYSTEM_ID, "+
        "JOB_ID, "+
        "LATEST_FLAG, "+
        "START_TIME_KEY, "+
        "END_TIME_KEY, "+
        "CONCAT(CUSTOMER_NUMBER, BANKING_TYPE, CAST(START_TIME_KEY AS STRING)) CUSTOMER_KEY "+
        "FROM ( "+
        "SELECT "+
        "Q.*, "+
        "ROW_NUMBER() OVER(PARTITION BY BANKING_TYPE,CUSTOMER_NUMBER,START_TIME_KEY,END_TIME_KEY ORDER BY LENGTH(CUSTOMER_NAME) DESC) AS RN "+
        "FROM ( "+
        "SELECT "+
        "NVL(SUBSTR(CUSTOMER_NR,INSTR(CUSTOMER_NR,'#')+1),CUSTOMER_NR) AS CUSTOMER_NUMBER, "+
        "CUSTOMER_SHORTNAME AS CUSTOMER_NAME, "+
        "CUST_NAME_ADDRESS_1, "+
        "CUST_NAME_ADDRESS_2, "+
        "CUST_NAME_ADDRESS_3, "+
        "CUST_NAME_ADDRESS_4, "+
        "IS_ACTIVE AS CUSTOMER_STATUS, "+
        "IS_PARENT_IND, "+
        "ACCOUNT_OFFICER, "+
        "NATIONALITY, "+
        "DOMICILE, "+
        "INTERCOMPANY, "+
        "CREDIT_WORTHINESS, "+
        "INDUSTRY_CODE AS CUSTOMER_SEGMENT_CODE, "+
        "CASE WHEN INSTR(CUSTOMER_NR, '#') <> 0 THEN SUBSTR(CUSTOMER_NR,0,INSTR(CUSTOMER_NR,'#')-1) "+
        "     ELSE 'NA' "+
        "     END BANKING_TYPE, "+
        "CUSTOMER_ATTRIBUTE1, "+
        "CUSTOMER_ATTRIBUTE2, "+
        "CUSTOMER_ATTRIBUTE3, "+
        "CUSTOMER_ATTRIBUTE4, "+
        "CUSTOMER_ATTRIBUTE5, "+
        "CUSTOMER_ATTRIBUTE6, "+
        "CUSTOMER_ATTRIBUTE7, "+
        "CUSTOMER_ATTRIBUTE8, "+
        "CUSTOMER_ATTRIBUTE9, "+
        "CUSTOMER_ATTRIBUTE10, "+
        "CUSTOMER_ATTRIBUTE11, "+
        "CUSTOMER_ATTRIBUTE12, "+
        "CUSTOMER_ATTRIBUTE13, "+
        "CUSTOMER_ATTRIBUTE14, "+
        "CUSTOMER_ATTRIBUTE15, "+
        "CUSTOMER_ATTRIBUTE16, "+
        "CUSTOMER_ATTRIBUTE17, "+
        "CUSTOMER_ATTRIBUTE18, "+
        "CUSTOMER_ATTRIBUTE19, "+
        "CUSTOMER_ATTRIBUTE20, "+
        "CUSTOMER_ATTRIBUTE21, "+
        "CUSTOMER_ATTRIBUTE22, "+
        "CUSTOMER_TELEPHONE_NR, "+
        "CUSTOMER_ATTRIBUTE23, "+
        "CUSTOMER_ATTRIBUTE24, "+
        "CUSTOMER_TYPE, "+
        "ECONOMIC_SECTOR, "+
        "GLOBAL_CUSTOMER, "+
        "CASE WHEN LENGTH(NACE_CODE)=1 THEN NULL ELSE NACE_CODE END NACE_CODE, "+
        "CASE WHEN LENGTH(MIS_MASTER)=1 THEN NULL ELSE MIS_MASTER END MIS_MASTER, "+
        "CAST(NVL(IS_REPO_CORE_MKT_PARTICIPANT,0) AS INT) IS_REPO_CORE_MKT_PARTICIPANT, "+
        "CAST(UNIX_TIMESTAMP(START_VALIDITY_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) START_VALIDITY_DATE, "+
        "CAST(UNIX_TIMESTAMP(END_VALIDITY_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) END_VALIDITY_DATE, "+
        "CASE WHEN LENGTH(LEI_CODE)=1 THEN NULL ELSE LEI_CODE END LEI_CODE, "+
        "CAST(NVL(IS_SME,0) AS INT) IS_SME, "+
        "CAST(NVL(IS_SPE,0) AS INT) IS_SPE, "+
        "CAST(NVL(IS_STUDENT,0) AS INT) IS_STUDENT, "+
        "CAST(NVL(IS_RATED,0) AS INT) IS_RATED, "+
        "CAST(NVL(IS_DEFAULTED,0) AS INT) IS_DEFAULTED, "+
        "CASE WHEN LENGTH(COVERAGE_TEAM)=1 THEN NULL ELSE COVERAGE_TEAM END AS COVERAGE_TEAM, "+
        "CASE WHEN LENGTH(CRD_IND)=1 THEN NULL ELSE CRD_IND END AS CRD_IND, "+
        "CASE WHEN LENGTH(SKYFALL_IND)=1 THEN NULL ELSE SKYFALL_IND END AS SKYFALL_IND, "+
        "GROUP_UID, "+
        "CASE WHEN LENGTH(GROUP_NAME)=1 THEN NULL ELSE GROUP_NAME END AS GROUP_NAME, "+
        "CASE WHEN LENGTH(GLOBAL_RM_CODE)=1 THEN NULL ELSE GLOBAL_RM_CODE END AS GLOBAL_RM_CODE, "+
        "CASE WHEN LENGTH(GLOBAL_RM_NAME)=1 THEN NULL ELSE GLOBAL_RM_NAME END AS GLOBAL_RM_NAME, "+
        "CASE WHEN LENGTH(LOCAL_RM_CODE)=1 THEN NULL ELSE LOCAL_RM_CODE END AS LOCAL_RM_CODE, "+
        "CASE WHEN LENGTH(LOCAL_RM_NAME)=1 THEN NULL ELSE LOCAL_RM_NAME END AS LOCAL_RM_NAME, "+
        "CUSTOMER_ATTRIBUTE9 SOURCE_SYSTEM_ID, "+
        "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddHHmmss') JOB_ID, "+
        "CASE WHEN TO_DATE(CAST(UNIX_TIMESTAMP(END_VALIDITY_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP))='9999-12-31' THEN 'Y' ELSE 'N' END LATEST_FLAG, "+
        "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(START_VALIDITY_DATE,'MMM dd yyyy HH:mm'), 'yyyyMMdd') AS INT) START_TIME_KEY, "+
        "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(END_VALIDITY_DATE,'MMM dd yyyy HH:mm'), 'yyyyMMdd') AS INT) END_TIME_KEY "+
        "FROM DIM_OSX_CUSTOMER_INTERIM "+
        "WHERE TRIM(CUSTOMER_ATTRIBUTE9) NOT IN ('FGB') "+
        ") Q "+
        ") WHERE RN=1"
    )
   
   println(time+" "+"DIM_OSX_CUSTOMER - TRANSFORMED DATASET FOR CUSTOMER WITH DUPLICATE REMOVAL CREATED") 
   
   println(time+" "+"DIM_OSX_CUSTOMER - WRITE STARTED TO PATH: "+ hdfs_osx_customer)     
   
   osx_customer_transformed.repartition(200)
                           .write
                           .mode("overwrite")
                           .format("parquet")
                           .option("compression","snappy")
                           .save(hdfs_osx_customer)
    
   println(time+" "+"DIM_OSX_CUSTOMER - WRITE COMPLETED")  
                           
  }
  
}
