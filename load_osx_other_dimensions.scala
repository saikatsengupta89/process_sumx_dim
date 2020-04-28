package load_dimension

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import sys.process._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType


import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.DoubleType

object load_osx_other_dim {
  
  def time:String={
    
    /* GET CURRENT DATESTAMP FOR LOG WRITING TO YARN */
    val timeformat= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    return timeformat.format(new Date()).toString()
  }
  
  def sparkConfig(time:String):SparkConf= {
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
   
  def main (args: Array[String]) {
        
    val sparkConf= sparkConfig(time)
    val sparkContext= new SparkContext(sparkConf)
    val sqlContext= new SQLContext(sparkContext)
    
 
    /* define file raw dimension file read path */
    val hdfs_outbound_structure_dimension   = "hdfs://dev1node01.fgb.ae:8020/raw/onesumx/daily/T_OUTBOUND_STRUCTURE_DIMENSION_FGB.DAT"
    val hdfs_outbound_account_code          = "hdfs://dev1node01.fgb.ae:8020/raw/onesumx/daily/T_OUTBOUND_ACCOUNT_CODE_FGB.DAT"
    val hdfs_outbound_profit_centre         = "hdfs://dev1node01.fgb.ae:8020/raw/onesumx/daily/T_OUTBOUND_PROFIT_CENTRE_FGB.DAT"
    
    /* define dimension write path */
    val hdfs_dim_onesumx_structure_out      = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_onesumx_structure_out"
    val hdfs_dim_onesumx_profit_centre_out  = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_onesumx_profit_centre_out"
    val hdfs_dim_onesumx_account_code_out   = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_onesumx_account_code_out"
    val hdfs_dim_osx_currency               = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_currency"
    val hdfs_dim_osx_coverage_geography     = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_coverage_geography"
    val hdfs_dim_osx_coverage_geography_mi  = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_coverage_geography_mi"
    val hdfs_dim_osx_customer_segment_crd   = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_customer_segment_crd"
    val hdfs_dim_osx_customer_segment_fpa   = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_customer_segment_fpa"
    val hdfs_dim_osx_customer_segment_mi    = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_customer_segment_mi"
    val hdfs_dim_osx_department             = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_department"
    val hdfs_dim_osx_department_mi          = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_department_mi"
    val hdfs_dim_osx_domain                 = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_domain"
    val hdfs_dim_osx_gl_accounts_fpa        = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_gl_accounts_fpa"
    val hdfs_dim_osx_gl_accounts_gfr        = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_gl_accounts_gfr"
    val hdfs_dim_osx_gl_accounts_mi         = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_gl_accounts_mi"
    val hdfs_dim_osx_legal_entity           = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_legal_entity"
    val hdfs_dim_osx_legal_entity_mi        = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_legal_entity_mi"
    val hdfs_dim_osx_product                = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_product"
    val hdfs_dim_osx_product_mi             = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_product_mi"
    val hdfs_dim_osx_profit_centre          = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_profit_centre"
    val hdfs_dim_osx_profit_centre_mi       = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/dim_osx_profit_centre_mi"
    
    
    val master_data_schema= StructType(Array(
        StructField("structure_name",StringType, true),
        StructField("structure_version_name",StringType, true),
        StructField("leaf_level",IntegerType, true),
        StructField("structure_id",IntegerType, true),
        StructField("structure_version_id",IntegerType, true),
        StructField("start_validity_date",StringType, true),
        StructField("end_validity_date",StringType, true),
        StructField("lvl1_code",StringType, true),
        StructField("lvl1_desc",StringType, true),
        StructField("lvl2_code",StringType, true),
        StructField("lvl2_desc",StringType, true),
        StructField("lvl3_code",StringType, true),
        StructField("lvl3_desc",StringType, true),
        StructField("lvl4_code",StringType, true),
        StructField("lvl4_desc",StringType, true),
        StructField("lvl5_code",StringType, true),
        StructField("lvl5_desc",StringType, true),
        StructField("lvl6_code",StringType, true),
        StructField("lvl6_desc",StringType, true),
        StructField("lvl7_code",StringType, true),
        StructField("lvl7_desc",StringType, true),
        StructField("lvl8_code",StringType, true),
        StructField("lvl8_desc",StringType, true),
        StructField("lvl9_code",StringType, true),
        StructField("lvl9_desc",StringType, true),
        StructField("lvl10_code",StringType, true),
        StructField("lvl10_desc",StringType, true),
        StructField("leaf_code",StringType, true),
        StructField("leaf_desc",StringType, true),
        StructField("last_modified",StringType, true),
        StructField("modified_by",StringType, true)
    ))
    
    val account_code_schema= StructType(Array(
        StructField("account_category",StringType, true),
        StructField("account_code",StringType, true),
        StructField("description",StringType, true),
        StructField("account_section",StringType, true),
        StructField("start_validity_date",StringType, true),
        StructField("end_validity_date",StringType, true),
        StructField("position_account",StringType, true),
        StructField("account_section_converse",StringType, true),
        StructField("to_be_revalued",StringType, true),
        StructField("account_type",StringType, true),
        StructField("is_internal_account",StringType, true),
        StructField("classification",StringType, true),
        StructField("principal_accrued",StringType, true),
        StructField("fab_ifrs9_flag",IntegerType, true),
        StructField("moodys_ifrs9_flag",IntegerType, true),
        StructField("last_modified",StringType, true),
        StructField("last_modified_by",StringType, true)
    ))
    
     val profit_centre_schema= StructType(Array(
        StructField("profit_centre",StringType, true),
        StructField("entity",StringType, true),
        StructField("description",StringType, true),
        StructField("type",StringType, true),
        StructField("active_indicator",StringType, true),
        StructField("capitalized_indicator",StringType, true),
        StructField("custom_character_field_1",StringType, true),
        StructField("region_level_1",StringType, true),
        StructField("country",StringType, true),
        StructField("region_level_2",StringType, true),
        StructField("custom_character_field_5",StringType, true),
        StructField("division",StringType, true),
        StructField("cof_vof_included",DoubleType, true),
        StructField("to_be_revalued",DoubleType, true),
        StructField("islamic_indicator",DoubleType, true),
        StructField("lp_included",DoubleType, true),
        StructField("custom_number_field_5",DoubleType, true),
        StructField("last_modified",StringType, true),
        StructField("modified_by",StringType, true)
    ))
    
    println (time+" "+"DIMENSION LOAD INITIATED")
    
    val master_data= sqlContext.read.format("com.databricks.spark.csv")
                               .option("header", "true")
                               .option("delimiter","~")
                               .schema(master_data_schema)
                               .load (hdfs_outbound_structure_dimension)
                               .toDF("structure_name",
                                     "structure_version_name",
                                     "leaf_level",
                                     "structure_id",
                                     "structure_version_id",
                                     "start_validity_date",
                                     "end_validity_date",
                                     "lvl1_code",
                                     "lvl1_desc",
                                     "lvl2_code",
                                     "lvl2_desc",
                                     "lvl3_code",
                                     "lvl3_desc",
                                     "lvl4_code",
                                     "lvl4_desc",
                                     "lvl5_code",
                                     "lvl5_desc",
                                     "lvl6_code",
                                     "lvl6_desc",
                                     "lvl7_code",
                                     "lvl7_desc",
                                     "lvl8_code",
                                     "lvl8_desc",
                                     "lvl9_code",
                                     "lvl9_desc",
                                     "lvl10_code",
                                     "lvl10_desc",
                                     "leaf_code",
                                     "leaf_desc",
                                     "last_modified",
                                     "modified_by"
                                    )
                                    
    
                                    
    val master_account_code = sqlContext.read.format("com.databricks.spark.csv")
                                         .option("header", "true")
                                         .option("delimiter","~")
                                         .schema(account_code_schema)
                                         .load (hdfs_outbound_account_code)
                                         .toDF("account_category",
                                               "account_code",
                                               "description",
                                               "account_section",
                                               "start_validity_date",
                                               "end_validity_date",
                                               "position_account",
                                               "account_section_converse",
                                               "to_be_revalued",
                                               "account_type",
                                               "is_internal_account",
                                               "classification",
                                               "principal_accrued",
                                               "fab_ifrs9_flag",
                                               "moodys_ifrs9_flag",
                                               "last_modified",
                                               "last_modified_by"
                                              )
                                              
                                              
    val master_profit_centre_cd = sqlContext.read.format("com.databricks.spark.csv")
                                             .option("header", "true")
                                             .option("delimiter","~")
                                             .schema(profit_centre_schema)
                                             .load (hdfs_outbound_profit_centre)
                                             .toDF("profit_centre",
                                                    "entity",
                                                    "description",
                                                    "type",
                                                    "active_indicator",
                                                    "capitalized_indicator",
                                                    "custom_character_field_1",
                                                    "region_level_1",
                                                    "country",
                                                    "region_level_2",
                                                    "custom_character_field_5",
                                                    "division",
                                                    "cof_vof_included",
                                                    "to_be_revalued",
                                                    "islamic_indicator",
                                                    "lp_included",
                                                    "custom_number_field_5",
                                                    "last_modified",
                                                    "modified_by"
                                                   )
    
    master_data.registerTempTable("MASTER_DIMENSION_DATA")
    master_account_code.registerTempTable("MASTER_ACCOUNT_CODE_DATA")
    master_profit_centre_cd.registerTempTable("MASTER_PROFIT_CENTRE_CD_DATA")
    
    
    val master_data_post_transform= sqlContext.sql(
        "SELECT "+ 
        "STRUCTURE_NAME, "+
        "STRUCTURE_VERSION_NAME, "+
        "CAST(LEAF_LEVEL AS INT) LEAF_LEVEL, "+
        "CAST(STRUCTURE_ID AS INT) STRUCTURE_ID, "+
        "CAST(STRUCTURE_VERSION_ID AS INT) STRUCTURE_VERSION_ID, "+
        "CONCAT( "+
        "CONCAT( "+
        "CONCAT(CONCAT(CASE WHEN CAST(REGEXP_REPLACE(SUBSTR(START_VALIDITY_DATE,5,2),' ','') AS INT) BETWEEN 1 AND 9 "+
        "                   THEN CONCAT('0',SUBSTR(START_VALIDITY_DATE,6,1)) ELSE SUBSTR(START_VALIDITY_DATE,5,2) END, "+
        "              '-') "+
        "       ,UPPER(SUBSTR(START_VALIDITY_DATE,0,3))), '-') "+
        "       ,SUBSTR(START_VALIDITY_DATE,8,4)) AS START_VALIDITY_DATE, "+
        "CONCAT( "+
        "CONCAT( "+
        "CONCAT(CONCAT(CASE WHEN CAST(REGEXP_REPLACE(SUBSTR(END_VALIDITY_DATE,5,2),' ','') AS INT) BETWEEN 1 AND 9 "+
        "                   THEN CONCAT('0',SUBSTR(END_VALIDITY_DATE,6,1)) ELSE SUBSTR(END_VALIDITY_DATE,5,2) END, "+
        "              '-') "+
        "       ,UPPER(SUBSTR(END_VALIDITY_DATE,0,3))), '-') "+
        "       ,SUBSTR(END_VALIDITY_DATE,8,4)) AS END_VALIDITY_DATE, "+
        "LVL1_CODE, "+
        "LVL1_DESC, "+
        "LVL2_CODE, "+
        "LVL2_DESC, "+
        "LVL3_CODE, "+
        "LVL3_DESC, "+
        "LVL4_CODE, "+
        "LVL4_DESC, "+
        "LVL5_CODE, "+
        "LVL5_DESC, "+
        "LVL6_CODE, "+
        "LVL6_DESC, "+
        "LVL7_CODE, "+
        "LVL7_DESC, "+
        "LVL8_CODE, "+
        "LVL8_DESC, "+
        "LVL9_CODE, "+
        "LVL9_DESC, "+
        "LVL10_CODE, "+
        "LVL10_DESC, "+
        "LEAF_CODE, "+
        "LEAF_DESC, "+
        "CONCAT( "+
        "CONCAT( "+
        "CONCAT(CONCAT(CASE WHEN CAST(REGEXP_REPLACE(SUBSTR(LAST_MODIFIED,5,2),' ','') AS INT) BETWEEN 1 AND 9 "+
        "                   THEN CONCAT('0',SUBSTR(LAST_MODIFIED,6,1)) ELSE SUBSTR(LAST_MODIFIED,5,2) END, "+
        "              '-') "+
        "       ,SUBSTR(LAST_MODIFIED,0,3)), '-') "+
        "       ,SUBSTR(LAST_MODIFIED,8,4)) AS LAST_MODIFIED, "+
        "MODIFIED_BY "+
        "FROM MASTER_DIMENSION_DATA"
        )  
        
    
    val master_account_code_data_interim= sqlContext.sql (
        "SELECT "+ 
        "ACCOUNT_CATEGORY, "+
        "ACCOUNT_CODE, "+
        "DESCRIPTION, "+
        "ACCOUNT_SECTION, "+
        "CONCAT( "+
        "CONCAT( "+
        "CONCAT(CONCAT(CASE WHEN CAST(REGEXP_REPLACE(SUBSTR(START_VALIDITY_DATE,5,2),' ','') AS INT) BETWEEN 1 AND 9 "+
        "                   THEN CONCAT('0',SUBSTR(START_VALIDITY_DATE,6,1)) ELSE SUBSTR(START_VALIDITY_DATE,5,2) END, "+
        "              '-') "+
        "       ,SUBSTR(START_VALIDITY_DATE,0,3)), '-') "+
        "       ,SUBSTR(START_VALIDITY_DATE,8,4)) AS START_VALIDITY_DATE, "+
        "CONCAT( "+
        "CONCAT( "+
        "CONCAT(CONCAT(CASE WHEN CAST(REGEXP_REPLACE(SUBSTR(END_VALIDITY_DATE,5,2),' ','') AS INT) BETWEEN 1 AND 9 "+
        "                   THEN CONCAT('0',SUBSTR(END_VALIDITY_DATE,6,1)) ELSE SUBSTR(END_VALIDITY_DATE,5,2) END, "+
        "              '-') "+
        "       ,SUBSTR(END_VALIDITY_DATE,0,3)), '-') "+
        "       ,SUBSTR(END_VALIDITY_DATE,8,4)) AS END_VALIDITY_DATE, "+
        "POSITION_ACCOUNT, "+
        "ACCOUNT_SECTION_CONVERSE, "+
        "TO_BE_REVALUED, "+
        "ACCOUNT_TYPE, "+
        "IS_INTERNAL_ACCOUNT, "+
        "CLASSIFICATION, "+
        "PRINCIPAL_ACCRUED, "+
        "CAST(FAB_IFRS9_FLAG AS INT) FAB_IFRS9_FLAG, "+
        "CAST(MOODYS_IFRS9_FLAG AS INT) MOODYS_IFRS9_FLAG "+
        "FROM MASTER_ACCOUNT_CODE_DATA"
    )
    
    master_account_code_data_interim.registerTempTable("MASTER_ACCOUNT_CODE_DATA_INT")
    
    val master_account_code_data_post_transform= sqlContext.sql (
        "SELECT "+ 
        "ACCOUNT_CATEGORY, "+
        "ACCOUNT_CODE, "+
        "DESCRIPTION, "+
        "ACCOUNT_SECTION, "+
        "START_VALIDITY_DATE, "+
        "END_VALIDITY_DATE, "+
        "POSITION_ACCOUNT, "+
        "ACCOUNT_SECTION_CONVERSE, "+
        "TO_BE_REVALUED, "+
        "ACCOUNT_TYPE, "+
        "IS_INTERNAL_ACCOUNT, "+
        "CLASSIFICATION, "+
        "PRINCIPAL_ACCRUED, "+
        "FAB_IFRS9_FLAG, "+
        "MOODYS_IFRS9_FLAG "+
        "FROM MASTER_ACCOUNT_CODE_DATA_INT "+
        "WHERE UNIX_TIMESTAMP(CURRENT_TIMESTAMP) BETWEEN UNIX_TIMESTAMP(START_VALIDITY_DATE,'dd-MMM-yyyy') "+
        "AND UNIX_TIMESTAMP(END_VALIDITY_DATE,'dd-MMM-yyyy')"
    )
      
    
    val master_profit_centre_cd_post_transform= sqlContext.sql (
        "SELECT "+
        "PROFIT_CENTRE, "+
        "ENTITY, "+
        "DESCRIPTION, "+
        "TYPE, "+
        "ACTIVE_INDICATOR, "+
        "CAPITALIZED_INDICATOR, "+
        "DIVISION, "+
        "REGION_LEVEL_1, "+
        "COUNTRY, "+
        "REGION_LEVEL_2, "+
        "CAST(COF_VOF_INCLUDED AS INT) COF_VOF_INCLUDED, "+
        "CAST(TO_BE_REVALUED AS INT) TO_BE_REVALUED, "+
        "CAST(ISLAMIC_INDICATOR AS INT) ISLAMIC_INDICATOR, "+
        "CAST(LP_INCLUDED AS INT) LP_INCLUDED, "+
        "CUSTOM_CHARACTER_FIELD_1, "+
        "CUSTOM_CHARACTER_FIELD_5, "+
        "CAST(CUSTOM_NUMBER_FIELD_5 AS INT) CUSTOM_NUMBER_FIELD_5 "+
        "FROM MASTER_PROFIT_CENTRE_CD_DATA"
    )
    
    /* PERSISTING THE TRANSFORMATION IN MEMORY FOR REGRESSIVE USAGE */
    master_data_post_transform.persist(StorageLevel.MEMORY_ONLY_SER)
    master_account_code_data_post_transform.persist(StorageLevel.MEMORY_ONLY_SER)
    master_profit_centre_cd_post_transform.persist(StorageLevel.MEMORY_ONLY_SER)
    
    println(time+" "+"DIM_OSX_COVERAGE_GEOGRAPHY DIMENSION written to path: "+ hdfs_dim_osx_coverage_geography)
    
    master_data_post_transform.registerTempTable("DIM_ONESUMX_STRUCTURE_OUT")
    master_account_code_data_post_transform.registerTempTable("DIM_ONESUMX_ACCOUNT_CODE_OUT")
    master_profit_centre_cd_post_transform.registerTempTable("DIM_ONESUMX_PROFIT_CENTRE_OUT")
    
    
    /* REFERENCE TABLE WRITTEN FOR RECON */
    
    master_data_post_transform.coalesce(1)
                              .write
                              .mode("overwrite")
                              .format("parquet")
                              .option("compression","snappy")
                              .save(hdfs_dim_onesumx_structure_out)
                              
    println(time+" "+"DIM_ONESUMX_STRUCTURE_OUT written to hdfs path: "+ hdfs_dim_onesumx_structure_out)
                              
    master_account_code_data_post_transform.coalesce(1)
                                           .write
                                           .mode("overwrite")
                                           .format("parquet")
                                           .option("compression","snappy")
                                           .save(hdfs_dim_onesumx_account_code_out)
    
    println(time+" "+"DIM_ONESUMX_ACCOUNT_CODE_OUT written to hdfs path: "+ hdfs_dim_onesumx_account_code_out)
    
    master_profit_centre_cd_post_transform.coalesce(1)
                                          .write
                                          .mode("overwrite")
                                          .format("parquet")
                                          .option("compression","snappy")
                                          .save(hdfs_dim_onesumx_profit_centre_out)
    
    println(time+" "+"DIM_ONESUMX_PROFIT_CENTRE_OUT written to hdfs path: "+ hdfs_dim_onesumx_profit_centre_out)
    
    val dim_osx_coverag_geography=
      sqlContext.sql (
        "SELECT "+
        "LEAF_CODE AS COVERAGE_GEOGRAPHY, "+
        "LVL1_CODE AS L1_CODE, "+
        "TRIM(LVL1_DESC) L1_DESC, "+
        "LVL2_CODE AS L2_CODE, "+
        "TRIM(LVL2_DESC) L2_DESC, "+
        "LVL3_CODE AS L3_CODE, "+
        "TRIM(LVL3_DESC) L3_DESC, "+
        "LVL4_CODE AS L4_CODE, "+
        "TRIM(LVL4_DESC) L4_DESC, "+
        "LVL5_CODE AS L5_CODE, "+
        "TRIM(LVL5_DESC) L5_DESC, "+
        "LVL6_CODE AS L6_CODE, "+
        "TRIM(LVL6_DESC) L6_DESC, "+
        "LVL7_CODE AS L7_CODE, "+
        "TRIM(LVL7_DESC) L7_DESC, "+
        "LVL8_CODE AS L8_CODE, "+
        "TRIM(LVL8_DESC) L8_DESC, "+
        "LVL9_CODE AS L9_CODE, "+
        "TRIM(LVL9_DESC) L9_DESC, "+
        "LVL10_CODE AS L10_CODE, "+
        "TRIM(LVL10_DESC) L10_DESC, "+
        "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID " +
        "FROM DIM_ONESUMX_STRUCTURE_OUT "+
        "WHERE STRUCTURE_NAME= 'Coverage_Geo_FPA' "+
        "AND (LVL1_CODE, LEAF_LEVEL) IN "+
        "(SELECT LVL1_CODE, MAX(LEAF_LEVEL) LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME= 'Coverage_Geo_FPA' GROUP BY LVL1_CODE)"
      )
    
    
    dim_osx_coverag_geography.coalesce(1)
                             .write
                             .mode("overwrite")
                             .format("parquet")
                             .option("compression","snappy")
                             .save(hdfs_dim_osx_coverage_geography)
    
    println(time+" "+"DIM_OSX_COVERAGE_GEOGRAPHY written to hdfs path: "+ hdfs_dim_osx_coverage_geography)                        
                             
    val dim_osx_coverag_geography_mi=
      sqlContext.sql (
        "SELECT "+
        "LEAF_CODE AS COVERAGE_GEOGRAPHY, "+
        "LVL1_CODE AS L1_CODE, "+
        "TRIM(LVL1_DESC) L1_DESC, "+
        "LVL2_CODE AS L2_CODE, "+
        "TRIM(LVL2_DESC) L2_DESC, "+
        "LVL3_CODE AS L3_CODE, "+
        "TRIM(LVL3_DESC) L3_DESC, "+
        "LVL4_CODE AS L4_CODE, "+
        "TRIM(LVL4_DESC) L4_DESC, "+
        "LVL5_CODE AS L5_CODE, "+
        "TRIM(LVL5_DESC) L5_DESC, "+
        "LVL6_CODE AS L6_CODE, "+
        "TRIM(LVL6_DESC) L6_DESC, "+
        "LVL7_CODE AS L7_CODE, "+
        "TRIM(LVL7_DESC) L7_DESC, "+
        "LVL8_CODE AS L8_CODE, "+
        "TRIM(LVL8_DESC) L8_DESC, "+
        "LVL9_CODE AS L9_CODE, "+
        "TRIM(LVL9_DESC) L9_DESC, "+
        "LVL10_CODE AS L10_CODE, "+
        "TRIM(LVL10_DESC) L10_DESC, "+
        "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID " +
        "FROM DIM_ONESUMX_STRUCTURE_OUT "+
        "WHERE STRUCTURE_NAME= 'Coverage Geography MI' "+
        "AND (LVL1_CODE, LEAF_LEVEL) IN "+
        "(SELECT LVL1_CODE, LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME= 'Coverage Geography MI' AND LEAF_LEVEL=10)"
      )
      
    
    dim_osx_coverag_geography_mi.coalesce(1)
                             .write
                             .mode("overwrite")
                             .format("parquet")
                             .option("compression","snappy")
                             .save(hdfs_dim_osx_coverage_geography_mi)
                             
    
    println(time+" "+"DIM_OSX_COVERAGE_GEOGRAPHY_MI written to hdfs path: "+ hdfs_dim_osx_coverage_geography_mi)                              
    
    val dim_osx_customer_segment_crd =
      sqlContext.sql (
        "SELECT "+
        "LEAF_CODE AS CUSTOMER_SEGMENT_CRD, "+
        "LVL1_CODE AS L1_CODE, "+
        "TRIM(LVL1_DESC) L1_DESC, "+
        "LVL2_CODE AS L2_CODE, "+
        "TRIM(LVL2_DESC) L2_DESC, "+
        "LVL3_CODE AS L3_CODE, "+
        "TRIM(LVL3_DESC) L3_DESC, "+
        "LVL4_CODE AS L4_CODE, "+
        "TRIM(LVL4_DESC) L4_DESC, "+
        "LVL5_CODE AS L5_CODE, "+
        "TRIM(LVL5_DESC) L5_DESC, "+
        "LVL6_CODE AS L6_CODE, "+
        "TRIM(LVL6_DESC) L6_DESC, "+
        "LVL7_CODE AS L7_CODE, "+
        "TRIM(LVL7_DESC) L7_DESC, "+
        "LVL8_CODE AS L8_CODE, "+
        "TRIM(LVL8_DESC) L8_DESC, "+
        "LVL9_CODE AS L9_CODE, "+
        "TRIM(LVL9_DESC) L9_DESC, "+
        "LVL10_CODE AS L10_CODE, "+
        "TRIM(LVL10_DESC) L10_DESC, "+
        "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID " +
        "FROM DIM_ONESUMX_STRUCTURE_OUT "+
        "WHERE STRUCTURE_NAME= 'Cust_Seg_CRD_FPA' "+
        "AND (LVL1_CODE, LEAF_LEVEL) IN "+
        "(SELECT LVL1_CODE, MAX(LEAF_LEVEL) LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME= 'Cust_Seg_CRD_FPA' GROUP BY LVL1_CODE)"
      )
    
      
    dim_osx_customer_segment_crd.coalesce(1)
                             .write
                             .mode("overwrite")
                             .format("parquet")
                             .option("compression","snappy")
                             .save(hdfs_dim_osx_customer_segment_crd)
    
    
    println(time+" "+"DIM_OSX_CUSTOMER_SEGMENT_CRD written to hdfs path: "+ hdfs_dim_osx_customer_segment_crd)                          
    
    val dim_osx_customer_segment_fpa=
      sqlContext.sql (
        "SELECT "+
        "LEAF_CODE AS CUSTOMER_SEGMENT_CODE, "+
        "LVL1_CODE AS L1_CODE, "+
        "TRIM(LVL1_DESC) L1_DESC, "+
        "LVL2_CODE AS L2_CODE, "+
        "TRIM(LVL2_DESC) L2_DESC, "+
        "LVL3_CODE AS L3_CODE, "+
        "TRIM(LVL3_DESC) L3_DESC, "+
        "LVL4_CODE AS L4_CODE, "+
        "TRIM(LVL4_DESC) L4_DESC, "+
        "LVL5_CODE AS L5_CODE, "+
        "TRIM(LVL5_DESC) L5_DESC, "+
        "LVL6_CODE AS L6_CODE, "+
        "TRIM(LVL6_DESC) L6_DESC, "+
        "LVL7_CODE AS L7_CODE, "+
        "TRIM(LVL7_DESC) L7_DESC, "+
        "LVL8_CODE AS L8_CODE, "+
        "TRIM(LVL8_DESC) L8_DESC, "+
        "LVL9_CODE AS L9_CODE, "+
        "TRIM(LVL9_DESC) L9_DESC, "+
        "LVL10_CODE AS L10_CODE, "+
        "TRIM(LVL10_DESC) L10_DESC, "+
        "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID " +
        "FROM DIM_ONESUMX_STRUCTURE_OUT "+
        "WHERE STRUCTURE_NAME= 'Customer_Segment_FPA' "+
        "AND (LVL1_CODE, LEAF_LEVEL) IN "+
        "(SELECT LVL1_CODE, MAX(LEAF_LEVEL) LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME= 'Customer_Segment_FPA' GROUP BY LVL1_CODE)"
      )
      
    dim_osx_customer_segment_fpa.coalesce(1)
                             .write
                             .mode("overwrite")
                             .format("parquet")
                             .option("compression","snappy")
                             .save(hdfs_dim_osx_customer_segment_fpa)
                             
    
    println(time+" "+"DIM_OSX_CUSTOMER_SEGMENT_FPA written to hdfs path: "+ hdfs_dim_osx_customer_segment_fpa)                         
                             
    val dim_osx_customer_segment_mi=
      sqlContext.sql (
        "SELECT "+
        "LEAF_CODE AS CUSTOMER_SEGMENT_CODE, "+
        "LVL1_CODE AS L1_CODE, "+
        "TRIM(LVL1_DESC) L1_DESC, "+
        "LVL2_CODE AS L2_CODE, "+
        "TRIM(LVL2_DESC) L2_DESC, "+
        "LVL3_CODE AS L3_CODE, "+
        "TRIM(LVL3_DESC) L3_DESC, "+
        "LVL4_CODE AS L4_CODE, "+
        "TRIM(LVL4_DESC) L4_DESC, "+
        "LVL5_CODE AS L5_CODE, "+
        "TRIM(LVL5_DESC) L5_DESC, "+
        "LVL6_CODE AS L6_CODE, "+
        "TRIM(LVL6_DESC) L6_DESC, "+
        "LVL7_CODE AS L7_CODE, "+
        "TRIM(LVL7_DESC) L7_DESC, "+
        "LVL8_CODE AS L8_CODE, "+
        "TRIM(LVL8_DESC) L8_DESC, "+
        "LVL9_CODE AS L9_CODE, "+
        "TRIM(LVL9_DESC) L9_DESC, "+
        "LVL10_CODE AS L10_CODE, "+
        "TRIM(LVL10_DESC) L10_DESC, "+
        "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID " +
        "FROM DIM_ONESUMX_STRUCTURE_OUT "+
        "WHERE STRUCTURE_NAME= 'Customer Segment MI' "+
        "AND (LVL1_CODE, LEAF_LEVEL) IN "+
        "(SELECT LVL1_CODE, MAX(LEAF_LEVEL) LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME= 'Customer Segment MI' GROUP BY LVL1_CODE)"
      )
      
    dim_osx_customer_segment_fpa.coalesce(1)
                             .write
                             .mode("overwrite")
                             .format("parquet")
                             .option("compression","snappy")
                             .save(hdfs_dim_osx_customer_segment_mi)
     
    println(time+" "+"DIM_OSX_CUSTOMER_SEGMENT_MI written to hdfs path: "+ hdfs_dim_osx_customer_segment_mi)
    
    val dim_osx_department= 
      sqlContext.sql (
        "SELECT "+
        "LEAF_CODE AS DEPARTMENT_ID, "+
        "TRIM(LEAF_DESC) AS DEPARTMENT_DESC, "+
        "LVL1_CODE AS L1_CODE, "+
        "TRIM(LVL1_DESC) L1_DESC, "+
        "LVL2_CODE AS L2_CODE, "+
        "TRIM(LVL2_DESC) L2_DESC, "+
        "LVL3_CODE AS L3_CODE, "+
        "TRIM(LVL3_DESC) L3_DESC, "+
        "LVL4_CODE AS L4_CODE, "+
        "TRIM(LVL4_DESC) L4_DESC, "+
        "LVL5_CODE AS L5_CODE, "+
        "TRIM(LVL5_DESC) L5_DESC, "+
        "LVL6_CODE AS L6_CODE, "+
        "TRIM(LVL6_DESC) L6_DESC, "+
        "LVL7_CODE AS L7_CODE, "+
        "TRIM(LVL7_DESC) L7_DESC, "+
        "LVL8_CODE AS L8_CODE, "+
        "TRIM(LVL8_DESC) L8_DESC, "+
        "LVL9_CODE AS L9_CODE, "+
        "TRIM(LVL9_DESC) L9_DESC, "+
        "LVL10_CODE AS L10_CODE, "+
        "TRIM(LVL10_DESC) L10_DESC, "+
        "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID " +
        "FROM DIM_ONESUMX_STRUCTURE_OUT "+
        "WHERE STRUCTURE_NAME= 'Department ID_FPA' "+
        "AND (LVL1_CODE, LEAF_LEVEL) IN "+
        "(SELECT LVL1_CODE, MAX(LEAF_LEVEL) LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME= 'Department ID_FPA' "+ 
        " GROUP BY LVL1_CODE)"
      )
    
     dim_osx_department.coalesce(1)
                       .write
                       .mode("overwrite")
                       .format("parquet")
                       .option("compression","snappy")
                       .save(hdfs_dim_osx_department)  
      
    println(time+" "+"DIM_OSX_DEPARTMENT written to hdfs path: "+ hdfs_dim_osx_department)
                       
    val dim_osx_department_mi= 
      sqlContext.sql (
        "SELECT "+
        "LEAF_CODE AS DEPARTMENT_ID, "+
        "TRIM(LEAF_DESC) AS DEPARTMENT_DESC, "+
        "LVL1_CODE AS L1_CODE, "+
        "TRIM(LVL1_DESC) L1_DESC, "+
        "LVL2_CODE AS L2_CODE, "+
        "TRIM(LVL2_DESC) L2_DESC, "+
        "LVL3_CODE AS L3_CODE, "+
        "TRIM(LVL3_DESC) L3_DESC, "+
        "LVL4_CODE AS L4_CODE, "+
        "TRIM(LVL4_DESC) L4_DESC, "+
        "LVL5_CODE AS L5_CODE, "+
        "TRIM(LVL5_DESC) L5_DESC, "+
        "LVL6_CODE AS L6_CODE, "+
        "TRIM(LVL6_DESC) L6_DESC, "+
        "LVL7_CODE AS L7_CODE, "+
        "TRIM(LVL7_DESC) L7_DESC, "+
        "LVL8_CODE AS L8_CODE, "+
        "TRIM(LVL8_DESC) L8_DESC, "+
        "LVL9_CODE AS L9_CODE, "+
        "TRIM(LVL9_DESC) L9_DESC, "+
        "LVL10_CODE AS L10_CODE, "+
        "TRIM(LVL10_DESC) L10_DESC, "+
        "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID " +
        "FROM DIM_ONESUMX_STRUCTURE_OUT "+
        "WHERE TRIM(STRUCTURE_NAME)= 'Department MI' "+
        "AND (LVL1_CODE, LEAF_LEVEL) IN "+
        "(SELECT LVL1_CODE, MAX(LEAF_LEVEL) LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE TRIM(STRUCTURE_NAME)= 'Department MI' "+ 
        " GROUP BY LVL1_CODE)"
      )
    
     dim_osx_department_mi.coalesce(1)
                       .write
                       .mode("overwrite")
                       .format("parquet")
                       .option("compression","snappy")
                       .save(hdfs_dim_osx_department_mi)
                       
                       
     println(time+" "+"DIM_OSX_DEPARTMENT_MI written to hdfs path: "+ hdfs_dim_osx_department_mi)
                       
      val dim_osx_domain=
         sqlContext.sql (
          "SELECT "+
          "LEAF_CODE AS DOMAIN_ID, "+
          "LEAF_DESC AS DOMAIN_NAME, "+
          "LEAF_DESC AS DOMAIN_DESC, "+
          "LVL1_CODE AS L1_CODE, "+
          "TRIM(LVL1_DESC) L1_DESC, "+
          "LVL2_CODE AS L2_CODE, "+
          "TRIM(LVL2_DESC) L2_DESC, "+
          "LVL3_CODE AS L3_CODE, "+
          "TRIM(LVL3_DESC) L3_DESC, "+
          "LVL4_CODE AS L4_CODE, "+
          "TRIM(LVL4_DESC) L4_DESC, "+
          "LVL5_CODE AS L5_CODE, "+
          "TRIM(LVL5_DESC) L5_DESC, "+
          "LVL6_CODE AS L6_CODE, "+
          "TRIM(LVL6_DESC) L6_DESC, "+
          "LVL7_CODE AS L7_CODE, "+
          "TRIM(LVL7_DESC) L7_DESC, "+
          "LVL8_CODE AS L8_CODE, "+
          "TRIM(LVL8_DESC) L8_DESC, "+
          "LVL9_CODE AS L9_CODE, "+
          "TRIM(LVL9_DESC) L9_DESC, "+
          "LVL10_CODE AS L10_CODE, "+
          "TRIM(LVL10_DESC) L10_DESC, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID " +
          "FROM DIM_ONESUMX_STRUCTURE_OUT "+
          "WHERE STRUCTURE_NAME= 'Domain' "+
          "AND (LVL1_CODE, LEAF_LEVEL) IN "+
          "(SELECT LVL1_CODE, MAX(LEAF_LEVEL) LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME= 'Domain' "+ 
          " GROUP BY LVL1_CODE)"
        )
      
      dim_osx_domain.coalesce(1)
                       .write
                       .mode("overwrite")
                       .format("parquet")
                       .option("compression","snappy")
                       .save(hdfs_dim_osx_domain) 
                       
    println(time+" "+"DIM_OSX_DOMAIN written to hdfs path: "+ hdfs_dim_osx_domain)
    
    val dim_osx_gl_accounts_fpa=
       sqlContext.sql (
          "SELECT "+
          "LEAF_CODE AS GL_ACCOUNT_ID, "+
          "LVL1_CODE AS GL_ACCOUNT_LEVL1_CODE, "+
          "TRIM(LVL1_DESC) AS GL_ACCOUNT_LEVL1_DESC, "+
          "LVL2_CODE AS GL_ACCOUNT_LEVL2_CODE, "+
          "TRIM(LVL2_DESC) AS GL_ACCOUNT_LEVL2_DESC, "+
          "LVL3_CODE AS GL_ACCOUNT_LEVL3_CODE, "+
          "TRIM(LVL3_DESC) AS GL_ACCOUNT_LEVL3_DESC, "+
          "LVL4_CODE AS GL_ACCOUNT_LEVL4_CODE, "+
          "TRIM(LVL4_DESC) AS GL_ACCOUNT_LEVL4_DESC, "+
          "LVL5_CODE AS GL_ACCOUNT_LEVL5_CODE, "+
          "TRIM(LVL5_DESC) AS GL_ACCOUNT_LEVL5_DESC, "+
          "LVL6_CODE AS GL_ACCOUNT_LEVL6_CODE, "+
          "TRIM(LVL6_DESC) AS GL_ACCOUNT_LEVL6_DESC, "+
          "LVL7_CODE AS GL_ACCOUNT_LEVL7_CODE, "+
          "TRIM(LVL7_DESC) AS GL_ACCOUNT_LEVL7_DESC, "+
          "LVL8_CODE AS GL_ACCOUNT_LEVL8_CODE, "+
          "TRIM(LVL8_DESC) AS GL_ACCOUNT_LEVL8_DESC, "+
          "LVL9_CODE AS GL_ACCOUNT_LEVL9_CODE, "+
          "TRIM(LVL9_DESC) AS GL_ACCOUNT_LEVL9_DESC, "+
          "LVL10_CODE AS GL_ACCOUNT_LEVL10_CODE, "+
          "TRIM(LVL10_DESC) AS GL_ACCOUNT_LEVL10_DESC, "+
          "TRIM(LEAF_DESC) AS DESCRIPTION, "+
          "CASE WHEN AC.ACCOUNT_SECTION IN ('AS','LI','CA','CL','NU') THEN 'BS' "+
          "     WHEN AC.ACCOUNT_SECTION IN ('IN','EX') THEN 'PL' "+
          "     ELSE NULL "+
          "END GL_PL_FLAG, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'dd-MMM-yyyy') LOAD_DATE, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID, " +
          "AC.ACCOUNT_CATEGORY, "+
          "AC.ACCOUNT_SECTION, "+
          "AC.START_VALIDITY_DATE, "+
          "AC.END_VALIDITY_DATE, "+
          "AC.POSITION_ACCOUNT, "+
          "AC.ACCOUNT_SECTION_CONVERSE, "+
          "AC.TO_BE_REVALUED, "+
          "AC.ACCOUNT_TYPE, "+
          "CAST(AC.IS_INTERNAL_ACCOUNT AS INT) IS_INTERNAL_ACCOUNT, "+
          "AC.CLASSIFICATION, "+
          "AC.PRINCIPAL_ACCRUED, "+
          "CAST(AC.FAB_IFRS9_FLAG AS INT) FAB_IFRS9_FLAG, "+
          "CAST(AC.MOODYS_IFRS9_FLAG AS INT) MOODYS_IFRS9_FLAG "+
          "FROM "+
          "(SELECT * FROM DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME = 'Account Code FPA' "+
          "AND ( "+
          "	(LVL1_CODE,LEAF_LEVEL) IN  "+
          "	(SELECT LVL1_CODE, MAX(LEAF_LEVEL)LEAF_LEVEL FROM DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME = 'Account Code FPA' GROUP BY LVL1_CODE) "+
          "	) "+
          ")STR "+
          "LEFT OUTER JOIN DIM_ONESUMX_ACCOUNT_CODE_OUT AC "+
          "ON STR.LEAF_CODE = AC.ACCOUNT_CODE"
        )
        
  
    dim_osx_gl_accounts_fpa.coalesce(1)
                   .write
                   .mode("overwrite")
                   .format("parquet")
                   .option("compression","snappy")
                   .save(hdfs_dim_osx_gl_accounts_fpa) 
    
                   
    println(time+" "+"DIM_OSX_GL_ACCOUNTS_FPA written to hdfs path: "+ hdfs_dim_osx_gl_accounts_fpa)
    
    val dim_osx_gl_accounts_gfr=
       sqlContext.sql (
          "SELECT "+
          "LEAF_CODE AS GL_ACCOUNT_ID, "+
          "LVL1_CODE AS GL_ACCOUNT_LEVL1_CODE, "+
          "TRIM(LVL1_DESC) AS GL_ACCOUNT_LEVL1_DESC, "+
          "LVL2_CODE AS GL_ACCOUNT_LEVL2_CODE, "+
          "TRIM(LVL2_DESC) AS GL_ACCOUNT_LEVL2_DESC, "+
          "LVL3_CODE AS GL_ACCOUNT_LEVL3_CODE, "+
          "TRIM(LVL3_DESC) AS GL_ACCOUNT_LEVL3_DESC, "+
          "LVL4_CODE AS GL_ACCOUNT_LEVL4_CODE, "+
          "TRIM(LVL4_DESC) AS GL_ACCOUNT_LEVL4_DESC, "+
          "LVL5_CODE AS GL_ACCOUNT_LEVL5_CODE, "+
          "TRIM(LVL5_DESC) AS GL_ACCOUNT_LEVL5_DESC, "+
          "LVL6_CODE AS GL_ACCOUNT_LEVL6_CODE, "+
          "TRIM(LVL6_DESC) AS GL_ACCOUNT_LEVL6_DESC, "+
          "LVL7_CODE AS GL_ACCOUNT_LEVL7_CODE, "+
          "TRIM(LVL7_DESC) AS GL_ACCOUNT_LEVL7_DESC, "+
          "LVL8_CODE AS GL_ACCOUNT_LEVL8_CODE, "+
          "TRIM(LVL8_DESC) AS GL_ACCOUNT_LEVL8_DESC, "+
          "LVL9_CODE AS GL_ACCOUNT_LEVL9_CODE, "+
          "TRIM(LVL9_DESC) AS GL_ACCOUNT_LEVL9_DESC, "+
          "LVL10_CODE AS GL_ACCOUNT_LEVL10_CODE, "+
          "TRIM(LVL10_DESC) AS GL_ACCOUNT_LEVL10_DESC, "+
          "TRIM(LEAF_DESC) AS DESCRIPTION, "+
          "CASE WHEN AC.ACCOUNT_SECTION IN ('AS','LI','CA','CL','NU') THEN 'BS' "+
          "     WHEN AC.ACCOUNT_SECTION IN ('IN','EX') THEN 'PL' "+
          "     ELSE NULL "+
          "END GL_PL_FLAG, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'dd-MMM-yyyy') LOAD_DATE, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID, " +
          "AC.ACCOUNT_CATEGORY, "+
          "AC.ACCOUNT_SECTION, "+
          "AC.START_VALIDITY_DATE, "+
          "AC.END_VALIDITY_DATE, "+
          "AC.POSITION_ACCOUNT, "+
          "AC.ACCOUNT_SECTION_CONVERSE, "+
          "AC.TO_BE_REVALUED, "+
          "AC.ACCOUNT_TYPE, "+
          "CAST(AC.IS_INTERNAL_ACCOUNT AS INT) IS_INTERNAL_ACCOUNT, "+
          "AC.CLASSIFICATION, "+
          "AC.PRINCIPAL_ACCRUED, "+
          "CAST(AC.FAB_IFRS9_FLAG AS INT) FAB_IFRS9_FLAG, "+
          "CAST(AC.MOODYS_IFRS9_FLAG AS INT) MOODYS_IFRS9_FLAG "+
          "FROM "+
          "(SELECT * FROM DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME = 'Account Code - Financial Reporting' "+
          "AND ( "+
          "	(LVL1_CODE,LEAF_LEVEL) IN  "+
          "	(SELECT LVL1_CODE, MAX(LEAF_LEVEL)LEAF_LEVEL FROM DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME = 'Account Code - Financial Reporting' "+
          "  GROUP BY LVL1_CODE) "+
          "	) "+
          ")STR "+
          "LEFT OUTER JOIN DIM_ONESUMX_ACCOUNT_CODE_OUT AC "+
          "ON STR.LEAF_CODE = AC.ACCOUNT_CODE"
        )
                   
    dim_osx_gl_accounts_gfr.coalesce(1)
                   .write
                   .mode("overwrite")
                   .format("parquet")
                   .option("compression","snappy")
                   .save(hdfs_dim_osx_gl_accounts_gfr)                
   
    println(time+" "+"DIM_OSX_GL_ACCOUNTS_GRF written to hdfs path: "+ hdfs_dim_osx_gl_accounts_gfr)               
                   
    val dim_osx_gl_accounts_mi=
       sqlContext.sql (
          "SELECT "+
          "LEAF_CODE AS GL_ACCOUNT_ID, "+
          "LVL1_CODE AS GL_ACCOUNT_LEVL1_CODE, "+
          "TRIM(LVL1_DESC) AS GL_ACCOUNT_LEVL1_DESC, "+
          "LVL2_CODE AS GL_ACCOUNT_LEVL2_CODE, "+
          "TRIM(LVL2_DESC) AS GL_ACCOUNT_LEVL2_DESC, "+
          "LVL3_CODE AS GL_ACCOUNT_LEVL3_CODE, "+
          "TRIM(LVL3_DESC) AS GL_ACCOUNT_LEVL3_DESC, "+
          "LVL4_CODE AS GL_ACCOUNT_LEVL4_CODE, "+
          "TRIM(LVL4_DESC) AS GL_ACCOUNT_LEVL4_DESC, "+
          "LVL5_CODE AS GL_ACCOUNT_LEVL5_CODE, "+
          "TRIM(LVL5_DESC) AS GL_ACCOUNT_LEVL5_DESC, "+
          "LVL6_CODE AS GL_ACCOUNT_LEVL6_CODE, "+
          "TRIM(LVL6_DESC) AS GL_ACCOUNT_LEVL6_DESC, "+
          "LVL7_CODE AS GL_ACCOUNT_LEVL7_CODE, "+
          "TRIM(LVL7_DESC) AS GL_ACCOUNT_LEVL7_DESC, "+
          "LVL8_CODE AS GL_ACCOUNT_LEVL8_CODE, "+
          "TRIM(LVL8_DESC) AS GL_ACCOUNT_LEVL8_DESC, "+
          "LVL9_CODE AS GL_ACCOUNT_LEVL9_CODE, "+
          "TRIM(LVL9_DESC) AS GL_ACCOUNT_LEVL9_DESC, "+
          "LVL10_CODE AS GL_ACCOUNT_LEVL10_CODE, "+
          "TRIM(LVL10_DESC) AS GL_ACCOUNT_LEVL10_DESC, "+
          "TRIM(LEAF_DESC) AS DESCRIPTION, "+
          "CASE WHEN AC.ACCOUNT_SECTION IN ('AS','LI','CA','CL','NU') THEN 'BS' "+
          "     WHEN AC.ACCOUNT_SECTION IN ('IN','EX') THEN 'PL' "+
          "     ELSE NULL "+
          "END GL_PL_FLAG, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'dd-MMM-yyyy') LOAD_DATE, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID, " +
          "AC.ACCOUNT_CATEGORY, "+
          "AC.ACCOUNT_SECTION, "+
          "AC.START_VALIDITY_DATE, "+
          "AC.END_VALIDITY_DATE, "+
          "AC.POSITION_ACCOUNT, "+
          "AC.ACCOUNT_SECTION_CONVERSE, "+
          "AC.TO_BE_REVALUED, "+
          "AC.ACCOUNT_TYPE, "+
          "CAST(AC.IS_INTERNAL_ACCOUNT AS INT) IS_INTERNAL_ACCOUNT, "+
          "AC.CLASSIFICATION, "+
          "AC.PRINCIPAL_ACCRUED, "+
          "CAST(AC.FAB_IFRS9_FLAG AS INT) FAB_IFRS9_FLAG, "+
          "CAST(AC.MOODYS_IFRS9_FLAG AS INT) MOODYS_IFRS9_FLAG "+
          "FROM "+
          "(SELECT * FROM DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME = 'Account Code MI' "+
          "AND ( "+
          "	(LVL1_CODE,LEAF_LEVEL) IN  "+
          "	(SELECT LVL1_CODE, MAX(LEAF_LEVEL)LEAF_LEVEL FROM DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME = 'Account Code MI' "+
          "  GROUP BY LVL1_CODE) "+
          "	) "+
          ")STR "+
          "LEFT OUTER JOIN DIM_ONESUMX_ACCOUNT_CODE_OUT AC "+
          "ON STR.LEAF_CODE = AC.ACCOUNT_CODE"
        )
                   
    dim_osx_gl_accounts_mi.coalesce(1)
                   .write
                   .mode("overwrite")
                   .format("parquet")
                   .option("compression","snappy")
                   .save(hdfs_dim_osx_gl_accounts_mi)
    
    println(time+" "+"DIM_OSX_GL_ACCOUNTS_MI written to hdfs path: "+ hdfs_dim_osx_gl_accounts_mi)
    
    val dim_osx_legal_entity= sqlContext.sql (
          "SELECT "+
          "LEAF_CODE AS LEGAL_ENTITY, "+
          "LEAF_DESC AS DOMAIN_NAME, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS LOAD_DATE, " +
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID, " +
          "LVL1_CODE AS LEGAL_ENTITY_LEVL1_CODE, "+
          "TRIM(LVL1_DESC) AS LEGAL_ENTITY_LEVL1_DESC, "+
          "LVL2_CODE AS LEGAL_ENTITY_LEVL2_CODE, "+
          "TRIM(LVL2_DESC) AS LEGAL_ENTITY_LEVL2_DESC, "+
          "LVL3_CODE AS LEGAL_ENTITY_LEVL3_CODE, "+
          "TRIM(LVL3_DESC) AS LEGAL_ENTITY_LEVL3_DESC, "+
          "LVL4_CODE AS LEGAL_ENTITY_LEVL4_CODE, "+
          "TRIM(LVL4_DESC) AS LEGAL_ENTITY_LEVL4_DESC, "+
          "LVL5_CODE AS LEGAL_ENTITY_LEVL5_CODE, "+
          "TRIM(LVL5_DESC) AS LEGAL_ENTITY_LEVL5_DESC, "+
          "LVL6_CODE AS LEGAL_ENTITY_LEVL6_CODE, "+
          "TRIM(LVL6_DESC) AS LEGAL_ENTITY_LEVL6_DESC, "+
          "LVL7_CODE AS LEGAL_ENTITY_LEVL7_CODE, "+
          "TRIM(LVL7_DESC) AS LEGAL_ENTITY_LEVL7_DESC, "+
          "LVL8_CODE AS LEGAL_ENTITY_LEVL8_CODE, "+
          "TRIM(LVL8_DESC) AS LEGAL_ENTITY_LEVL8_DESC, "+
          "LVL9_CODE AS LEGAL_ENTITY_LEVL9_CODE, "+
          "TRIM(LVL9_DESC) AS LEGAL_ENTITY_LEVL9_DESC, "+
          "LVL10_CODE AS LEGAL_ENTITY_LEVL10_CODE, "+
          "TRIM(LVL10_DESC) AS LEGAL_ENTITY_LEVL10_DESC, "+
          "CAST(NULL AS STRING) CONSOLIDATION_FLAG "+
          "FROM DIM_ONESUMX_STRUCTURE_OUT "+
          "WHERE STRUCTURE_NAME= 'Entity FPA' "+
          "AND (LVL1_CODE, LEAF_LEVEL) IN "+
          "(SELECT LVL1_CODE, MAX(LEAF_LEVEL) LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME= 'Entity FPA' "+ 
          " GROUP BY LVL1_CODE)"
    )
    
    dim_osx_legal_entity.coalesce(1)
                         .write
                         .mode("overwrite")
                         .format("parquet")
                         .option("compression","snappy")
                         .save(hdfs_dim_osx_legal_entity)
    
    println(time+" "+"DIM_OSX_LEGAL_ENTITY written to hdfs path: "+ hdfs_dim_osx_legal_entity)
    
    val dim_osx_legal_entity_mi= sqlContext.sql (
          "SELECT "+
          "LEAF_CODE AS LEGAL_ENTITY, "+
          "LEAF_DESC AS DOMAIN_NAME, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS LOAD_DATE, " +
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID, " +
          "LVL1_CODE AS LEGAL_ENTITY_LEVL1_CODE, "+
          "TRIM(LVL1_DESC) AS LEGAL_ENTITY_LEVL1_DESC, "+
          "LVL2_CODE AS LEGAL_ENTITY_LEVL2_CODE, "+
          "TRIM(LVL2_DESC) AS LEGAL_ENTITY_LEVL2_DESC, "+
          "LVL3_CODE AS LEGAL_ENTITY_LEVL3_CODE, "+
          "TRIM(LVL3_DESC) AS LEGAL_ENTITY_LEVL3_DESC, "+
          "LVL4_CODE AS LEGAL_ENTITY_LEVL4_CODE, "+
          "TRIM(LVL4_DESC) AS LEGAL_ENTITY_LEVL4_DESC, "+
          "LVL5_CODE AS LEGAL_ENTITY_LEVL5_CODE, "+
          "TRIM(LVL5_DESC) AS LEGAL_ENTITY_LEVL5_DESC, "+
          "LVL6_CODE AS LEGAL_ENTITY_LEVL6_CODE, "+
          "TRIM(LVL6_DESC) AS LEGAL_ENTITY_LEVL6_DESC, "+
          "LVL7_CODE AS LEGAL_ENTITY_LEVL7_CODE, "+
          "TRIM(LVL7_DESC) AS LEGAL_ENTITY_LEVL7_DESC, "+
          "LVL8_CODE AS LEGAL_ENTITY_LEVL8_CODE, "+
          "TRIM(LVL8_DESC) AS LEGAL_ENTITY_LEVL8_DESC, "+
          "LVL9_CODE AS LEGAL_ENTITY_LEVL9_CODE, "+
          "TRIM(LVL9_DESC) AS LEGAL_ENTITY_LEVL9_DESC, "+
          "LVL10_CODE AS LEGAL_ENTITY_LEVL10_CODE, "+
          "TRIM(LVL10_DESC) AS LEGAL_ENTITY_LEVL10_DESC, "+
          "CAST(NULL AS STRING) CONSOLIDATION_FLAG "+
          "FROM DIM_ONESUMX_STRUCTURE_OUT "+
          "WHERE STRUCTURE_NAME= 'Entity MI' "+
          "AND (LVL1_CODE, LEAF_LEVEL) IN "+
          "(SELECT LVL1_CODE, MAX(LEAF_LEVEL) LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME= 'Entity MI' "+ 
          " GROUP BY LVL1_CODE)"
    )
    
    dim_osx_legal_entity_mi.coalesce(1)
                         .write
                         .mode("overwrite")
                         .format("parquet")
                         .option("compression","snappy")
                         .save(hdfs_dim_osx_legal_entity_mi)
    
     println(time+" "+"DIM_OSX_LEGAL_ENTITY_MI written to hdfs path: "+ hdfs_dim_osx_legal_entity_mi)
                         
     val dim_osx_product= sqlContext.sql (
         "SELECT "+
          "LEAF_CODE AS PRODUCT_CODE, "+
          "LEAF_DESC AS PRODUCT_DESCRIPTION, "+
          "CAST(NULL AS STRING) AS RETAIL_CORP, "+
          "CAST(NULL AS STRING) AS PRODUCTTYPE, "+
          "START_VALIDITY_DATE AS START_DT, "+
          "END_VALIDITY_DATE AS END_DT, "+
          "CAST(NULL AS STRING) AS ENABLED_FLAG, "+
          "CAST(NULL AS STRING) AS PRODUCT_TYPE_CD, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS LOAD_DATE, " +
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID, " +
          "CAST(NULL AS STRING) AS COMPANY_CODE, "+
          "CAST(NULL AS STRING) AS GL_PL_FLAG, "+
          "LVL1_CODE AS PRODUCT_LEVL1_CODE, "+
          "TRIM(LVL1_DESC) AS PRODUCT_LEVL1_DESC, "+
          "LVL2_CODE AS PRODUCT_LEVL2_CODE, "+
          "TRIM(LVL2_DESC) AS PRODUCT_LEVL2_DESC, "+
          "LVL3_CODE AS PRODUCT_LEVL3_CODE, "+
          "TRIM(LVL3_DESC) AS PRODUCT_LEVL3_DESC, "+
          "LVL4_CODE AS PRODUCT_LEVL4_CODE, "+
          "TRIM(LVL4_DESC) AS PRODUCT_LEVL4_DESC, "+
          "LVL5_CODE AS PRODUCT_LEVL5_CODE, "+
          "TRIM(LVL5_DESC) AS PRODUCT_LEVL5_DESC, "+
          "LVL6_CODE AS PRODUCT_LEVL6_CODE, "+
          "TRIM(LVL6_DESC) AS PRODUCT_LEVL6_DESC, "+
          "LVL7_CODE AS PRODUCT_LEVL7_CODE, "+
          "TRIM(LVL7_DESC) AS PRODUCT_LEVL7_DESC, "+
          "LVL8_CODE AS PRODUCT_LEVL8_CODE, "+
          "TRIM(LVL8_DESC) AS PRODUCT_LEVL8_DESC, "+
          "LVL9_CODE AS PRODUCT_LEVL9_CODE, "+
          "TRIM(LVL9_DESC) AS PRODUCT_LEVL9_DESC, "+
          "LVL10_CODE AS PRODUCT_LEVL10_CODE, "+
          "TRIM(LVL10_DESC) AS PRODUCT_LEVL10_DESC, "+
          "CAST(NULL AS STRING) CONSOLIDATION_FLAG "+
          "FROM DIM_ONESUMX_STRUCTURE_OUT "+
          "WHERE STRUCTURE_NAME= 'Product_FPA' "+
          "AND (LVL1_CODE, LEAF_LEVEL) IN "+
          "(SELECT LVL1_CODE, MAX(LEAF_LEVEL) LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME= 'Product_FPA' "+ 
          " GROUP BY LVL1_CODE)"
     )
     
     dim_osx_product.coalesce(1)
                     .write
                     .mode("overwrite")
                     .format("parquet")
                     .option("compression","snappy")
                     .save(hdfs_dim_osx_product)
     
     println(time+" "+"DIM_OSX_PRODUCT written to hdfs path: "+ hdfs_dim_osx_product)
                     
     val dim_osx_product_mi= sqlContext.sql (
         "SELECT "+
          "LEAF_CODE AS PRODUCT_CODE, "+
          "LEAF_DESC AS PRODUCT_DESCRIPTION, "+
          "CAST(NULL AS STRING) AS RETAIL_CORP, "+
          "CAST(NULL AS STRING) AS PRODUCTTYPE, "+
          "START_VALIDITY_DATE AS START_DT, "+
          "END_VALIDITY_DATE AS END_DT, "+
          "CAST(NULL AS STRING) AS ENABLED_FLAG, "+
          "CAST(NULL AS STRING) AS PRODUCT_TYPE_CD, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS LOAD_DATE, " +
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') AS JOB_ID, " +
          "CAST(NULL AS STRING) AS COMPANY_CODE, "+
          "CAST(NULL AS STRING) AS GL_PL_FLAG, "+
          "LVL1_CODE AS PRODUCT_LEVL1_CODE, "+
          "TRIM(LVL1_DESC) AS PRODUCT_LEVL1_DESC, "+
          "LVL2_CODE AS PRODUCT_LEVL2_CODE, "+
          "TRIM(LVL2_DESC) AS PRODUCT_LEVL2_DESC, "+
          "LVL3_CODE AS PRODUCT_LEVL3_CODE, "+
          "TRIM(LVL3_DESC) AS PRODUCT_LEVL3_DESC, "+
          "LVL4_CODE AS PRODUCT_LEVL4_CODE, "+
          "TRIM(LVL4_DESC) AS PRODUCT_LEVL4_DESC, "+
          "LVL5_CODE AS PRODUCT_LEVL5_CODE, "+
          "TRIM(LVL5_DESC) AS PRODUCT_LEVL5_DESC, "+
          "LVL6_CODE AS PRODUCT_LEVL6_CODE, "+
          "TRIM(LVL6_DESC) AS PRODUCT_LEVL6_DESC, "+
          "LVL7_CODE AS PRODUCT_LEVL7_CODE, "+
          "TRIM(LVL7_DESC) AS PRODUCT_LEVL7_DESC, "+
          "LVL8_CODE AS PRODUCT_LEVL8_CODE, "+
          "TRIM(LVL8_DESC) AS PRODUCT_LEVL8_DESC, "+
          "LVL9_CODE AS PRODUCT_LEVL9_CODE, "+
          "TRIM(LVL9_DESC) AS PRODUCT_LEVL9_DESC, "+
          "LVL10_CODE AS PRODUCT_LEVL10_CODE, "+
          "TRIM(LVL10_DESC) AS PRODUCT_LEVL10_DESC, "+
          "CAST(NULL AS STRING) CONSOLIDATION_FLAG "+
          "FROM DIM_ONESUMX_STRUCTURE_OUT "+
          "WHERE STRUCTURE_NAME= 'Product MI' "+
          "AND (LVL1_CODE, LEAF_LEVEL) IN "+
          "(SELECT LVL1_CODE, MAX(LEAF_LEVEL) LEAF_LEVEL FROM  DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME= 'Product MI' "+ 
          " GROUP BY LVL1_CODE)"
     )
     
     dim_osx_product_mi.coalesce(1)
                     .write
                     .mode("overwrite")
                     .format("parquet")
                     .option("compression","snappy")
                     .save(hdfs_dim_osx_product_mi)
                     
     
     println(time+" "+"DIM_OSX_PRODUCT_MI written to hdfs path: "+ hdfs_dim_osx_product_mi)
                     
     val dim_osx_profit_centre= sqlContext.sql (
          "SELECT "+
          "NVL(SUBSTR(LEAF_CODE, 1, INSTR(LEAF_CODE, '-')-1),LEAF_CODE) PROFIT_CENTRE_CD, "+
          "TRIM(LEAF_DESC) PROFIT_CENTRE_DESC, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') LOAD_DATE, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') JOB_ID, "+
          "NVL(SUBSTR(LVL1_CODE, 1, INSTR(LVL1_CODE, '-')-1),LVL1_CODE)PROFIT_CENTRE_LEVL1_CODE,"+
          "TRIM(LVL1_DESC) PROFIT_CENTRE_LEVL1_DESC, "+
          "NVL(SUBSTR(LVL2_CODE, 1, INSTR(LVL2_CODE, '-')-1),LVL2_CODE) PROFIT_CENTRE_LEVL2_CODE, "+
          "TRIM(LVL2_DESC) PROFIT_CENTRE_LEVL2_DESC, "+
          "NVL(SUBSTR(LVL3_CODE, 1, INSTR(LVL3_CODE, '-')-1),LVL3_CODE) PROFIT_CENTRE_LEVL3_CODE, "+
          "TRIM(LVL3_DESC) PROFIT_CENTRE_LEVL3_DESC, "+
          "NVL(SUBSTR(LVL4_CODE, 1, INSTR(LVL4_CODE, '-')-1),LVL4_CODE) PROFIT_CENTRE_LEVL4_CODE, "+
          "TRIM(LVL4_DESC) PROFIT_CENTRE_LEVL4_DESC, "+
          "NVL(SUBSTR(LVL5_CODE, 1, INSTR(LVL5_CODE, '-')-1),LVL5_CODE) PROFIT_CENTRE_LEVL5_CODE, "+
          "TRIM(LVL5_DESC) PROFIT_CENTRE_LEVL5_DESC, "+
          "NVL(SUBSTR(LVL6_CODE, 1, INSTR(LVL6_CODE, '-')-1),LVL6_CODE) PROFIT_CENTRE_LEVL6_CODE, "+
          "TRIM(LVL6_DESC) PROFIT_CENTRE_LEVL6_DESC, "+
          "NVL(SUBSTR(LVL7_CODE, 1, INSTR(LVL7_CODE, '-')-1),LVL7_CODE) PROFIT_CENTRE_LEVL7_CODE, "+
          "TRIM(LVL7_DESC) PROFIT_CENTRE_LEVL7_DESC, "+
          "NVL(SUBSTR(LVL8_CODE, 1, INSTR(LVL8_CODE, '-')-1),LVL8_CODE) PROFIT_CENTRE_LEVL8_CODE, "+
          "TRIM(LVL8_DESC) PROFIT_CENTRE_LEVL8_DESC, "+
          "NVL(SUBSTR(LVL9_CODE, 1, INSTR(LVL9_CODE, '-')-1),LVL9_CODE) PROFIT_CENTRE_LEVL9_CODE, "+
          "TRIM(LVL9_DESC) PROFIT_CENTRE_LEVL9_DESC, "+
          "NVL(SUBSTR(LVL10_CODE, 1, INSTR(LVL10_CODE, '-')-1),LVL10_CODE) PROFIT_CENTRE_LEVL10_CODE, "+
          "TRIM(LVL10_DESC) PROFIT_CENTRE_LEVL10_DESC, "+
          "ENTITY, "+
          "TYPE, "+
          "ACTIVE_INDICATOR, "+
          "CAPITALIZED_INDICATOR, "+
          "DIVISION, "+
          "REGION_LEVEL_1, "+
          "COUNTRY, "+
          "REGION_LEVEL_2, "+
          "CAST(COF_VOF_INCLUDED AS INT) COF_VOF_INCLUDED, "+
          "CAST(TO_BE_REVALUED AS INT) TO_BE_REVALUED, "+
          "CAST(ISLAMIC_INDICATOR AS INT) ISLAMIC_INDICATOR, "+
          "CAST(LP_INCLUDED AS INT) LP_INCLUDED, "+
          "CUSTOM_CHARACTER_FIELD_1, "+
          "CUSTOM_CHARACTER_FIELD_5, "+
          "CAST(CUSTOM_NUMBER_FIELD_5 AS INT) CUSTOM_NUMBER_FIELD_5 "+
          "FROM   "+
          "(SELECT * FROM DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME = 'Branch FPA' "+
          "AND ( "+
          "  (LVL1_CODE,LEAF_LEVEL) IN  "+
          "  (SELECT LVL1_CODE,MAX(LEAF_LEVEL)LEAF_LEVEL FROM DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME = 'Branch FPA' GROUP BY LVL1_CODE) "+
          "  ) "+
          ")STR "+
          "LEFT OUTER JOIN DIM_ONESUMX_PROFIT_CENTRE_OUT PC "+
          "ON STR.LEAF_CODE = PC.PROFIT_CENTRE||'-'||PC.ENTITY"
     )
     
     dim_osx_profit_centre.coalesce(1)
                     .write
                     .mode("overwrite")
                     .format("parquet")
                     .option("compression","snappy")
                     .save(hdfs_dim_osx_profit_centre)
     
                     
     println(time+" "+"DIM_OSX_PROFIT_CENTRE written to hdfs path: "+ hdfs_dim_osx_profit_centre)
     
     
     val dim_osx_profit_centre_mi= sqlContext.sql (
          "SELECT "+
          "NVL(SUBSTR(LEAF_CODE, 1, INSTR(LEAF_CODE, '-')-1),LEAF_CODE) PROFIT_CENTRE_CD, "+
          "TRIM(LEAF_DESC) PROFIT_CENTRE_DESC, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') LOAD_DATE, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddhhmmss') JOB_ID, "+
          "NVL(SUBSTR(LVL1_CODE, 1, INSTR(LVL1_CODE, '-')-1),LVL1_CODE)PROFIT_CENTRE_LEVL1_CODE,"+
          "TRIM(LVL1_DESC) PROFIT_CENTRE_LEVL1_DESC, "+
          "NVL(SUBSTR(LVL2_CODE, 1, INSTR(LVL2_CODE, '-')-1),LVL2_CODE) PROFIT_CENTRE_LEVL2_CODE, "+
          "TRIM(LVL2_DESC) PROFIT_CENTRE_LEVL2_DESC, "+
          "NVL(SUBSTR(LVL3_CODE, 1, INSTR(LVL3_CODE, '-')-1),LVL3_CODE) PROFIT_CENTRE_LEVL3_CODE, "+
          "TRIM(LVL3_DESC) PROFIT_CENTRE_LEVL3_DESC, "+
          "NVL(SUBSTR(LVL4_CODE, 1, INSTR(LVL4_CODE, '-')-1),LVL4_CODE) PROFIT_CENTRE_LEVL4_CODE, "+
          "TRIM(LVL4_DESC) PROFIT_CENTRE_LEVL4_DESC, "+
          "NVL(SUBSTR(LVL5_CODE, 1, INSTR(LVL5_CODE, '-')-1),LVL5_CODE) PROFIT_CENTRE_LEVL5_CODE, "+
          "TRIM(LVL5_DESC) PROFIT_CENTRE_LEVL5_DESC, "+
          "NVL(SUBSTR(LVL6_CODE, 1, INSTR(LVL6_CODE, '-')-1),LVL6_CODE) PROFIT_CENTRE_LEVL6_CODE, "+
          "TRIM(LVL6_DESC) PROFIT_CENTRE_LEVL6_DESC, "+
          "NVL(SUBSTR(LVL7_CODE, 1, INSTR(LVL7_CODE, '-')-1),LVL7_CODE) PROFIT_CENTRE_LEVL7_CODE, "+
          "TRIM(LVL7_DESC) PROFIT_CENTRE_LEVL7_DESC, "+
          "NVL(SUBSTR(LVL8_CODE, 1, INSTR(LVL8_CODE, '-')-1),LVL8_CODE) PROFIT_CENTRE_LEVL8_CODE, "+
          "TRIM(LVL8_DESC) PROFIT_CENTRE_LEVL8_DESC, "+
          "NVL(SUBSTR(LVL9_CODE, 1, INSTR(LVL9_CODE, '-')-1),LVL9_CODE) PROFIT_CENTRE_LEVL9_CODE, "+
          "TRIM(LVL9_DESC) PROFIT_CENTRE_LEVL9_DESC, "+
          "NVL(SUBSTR(LVL10_CODE, 1, INSTR(LVL10_CODE, '-')-1),LVL10_CODE) PROFIT_CENTRE_LEVL10_CODE, "+
          "TRIM(LVL10_DESC) PROFIT_CENTRE_LEVL10_DESC, "+
          "ENTITY, "+
          "TYPE, "+
          "ACTIVE_INDICATOR, "+
          "CAPITALIZED_INDICATOR, "+
          "DIVISION, "+
          "REGION_LEVEL_1, "+
          "COUNTRY, "+
          "REGION_LEVEL_2, "+
          "CAST(COF_VOF_INCLUDED AS INT) COF_VOF_INCLUDED, "+
          "CAST(TO_BE_REVALUED AS INT) TO_BE_REVALUED, "+
          "CAST(ISLAMIC_INDICATOR AS INT) ISLAMIC_INDICATOR, "+
          "CAST(LP_INCLUDED AS INT) LP_INCLUDED, "+
          "CUSTOM_CHARACTER_FIELD_1, "+
          "CUSTOM_CHARACTER_FIELD_5, "+
          "CAST(CUSTOM_NUMBER_FIELD_5 AS INT) CUSTOM_NUMBER_FIELD_5 "+
          "FROM   "+
          "(SELECT * FROM DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME = 'Profit Centre MI' "+
          "AND ( "+
          "  (LVL1_CODE,LEAF_LEVEL) IN  "+
          "  (SELECT LVL1_CODE,MAX(LEAF_LEVEL)LEAF_LEVEL FROM DIM_ONESUMX_STRUCTURE_OUT WHERE STRUCTURE_NAME = 'Profit Centre MI' GROUP BY LVL1_CODE) "+
          "  ) "+
          ")STR "+
          "LEFT OUTER JOIN DIM_ONESUMX_PROFIT_CENTRE_OUT PC "+
          "ON STR.LEAF_CODE = PC.PROFIT_CENTRE||'-'||PC.ENTITY"
     )
     
     dim_osx_profit_centre_mi.coalesce(1)
                     .write
                     .mode("overwrite")
                     .format("parquet")
                     .option("compression","snappy")
                     .save(hdfs_dim_osx_profit_centre_mi)
     
     println(time+" "+"DIM_OSX_PROFIT_CENTRE_MI written to hdfs path: "+ hdfs_dim_osx_profit_centre_mi)
     
     
     load_osx_kpi_measure.load_osx_kpi_data(sparkContext, sqlContext, dim_osx_gl_accounts_mi, dim_osx_department_mi, dim_osx_profit_centre_mi)
                         
     sparkContext.stop()
  }
  
}
