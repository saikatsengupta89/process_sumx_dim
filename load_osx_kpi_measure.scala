package load_dimension

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.DataFrame

object load_osx_kpi_measure {
  
  def load_osx_kpi_data (sc:SparkContext, 
                         sqlContext: SQLContext, 
                         dim_osx_gl_accounts_mi:DataFrame, 
                         dim_osx_department_mi:DataFrame,
                         dim_osx_profit_centre_mi:DataFrame){
    
    val hdfs_parameter_tbl                  = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/parameter_tbl"
    val hdfs_kpi_measure_department_map     = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/kpi_measure_department_map"
    val hdfs_kpi_measure_profit_centre_map  = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/kpi_measure_profit_centre_map"
    val hdfs_kpi_measure_gl_map             = "hdfs://dev1node01.fgb.ae:8020/data/fin_onesumx/kpi_measure_gl_map"
    
    val parameter_tbl= sqlContext.read.parquet(hdfs_parameter_tbl)
    parameter_tbl.registerTempTable("PARAMETER_TBL")
    
    dim_osx_gl_accounts_mi.registerTempTable("DIM_OSX_GL_ACCOUNTS_MI")
    dim_osx_department_mi.registerTempTable("DIM_OSX_DEPARTMENT_MI")
    dim_osx_profit_centre_mi.registerTempTable("DIM_OSX_PROFIT_CENTRE_MI")
    
    val kpi_measure_gl_map= sqlContext.sql (
        "SELECT "+
        "DISTINCT "+
        "AB.ATTRIBUTES,GL.GL_ACCOUNT_ID,AB.CATEGORY "+
        "FROM "+
        "DIM_OSX_GL_ACCOUNTS_MI GL "+
        "INNER JOIN PARAMETER_TBL AB "+
        "ON "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL1,'NA')||';', ';'||NVL(GL.GL_ACCOUNT_LEVL1_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'GL' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL2,'NA')||';', ';'||NVL(GL.GL_ACCOUNT_LEVL2_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'GL' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL3,'NA')||';', ';'||NVL(GL.GL_ACCOUNT_LEVL3_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'GL' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL4,'NA')||';', ';'||NVL(GL.GL_ACCOUNT_LEVL4_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'GL' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL5,'NA')||';', ';'||NVL(GL.GL_ACCOUNT_LEVL5_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'GL' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL6,'NA')||';', ';'||NVL(GL.GL_ACCOUNT_LEVL6_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'GL' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL7,'NA')||';', ';'||NVL(GL.GL_ACCOUNT_LEVL7_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'GL' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL8,'NA')||';', ';'||NVL(GL.GL_ACCOUNT_LEVL8_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'GL' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL9,'NA')||';', ';'||NVL(GL.GL_ACCOUNT_LEVL9_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'GL' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL10,'NA')||';', ';'||NVL(GL.GL_ACCOUNT_LEVL10_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'GL'"
    )
    
    
    kpi_measure_gl_map.coalesce(1)
                      .write
                      .mode("overwrite")
                      .format("parquet")
                      .option("compression","snappy")
                      .save(hdfs_kpi_measure_gl_map)
        
    println(load_osx_other_dim.time+" "+"KPI_MEASURE_GL_MAP written to hdfs path: "+ hdfs_kpi_measure_gl_map)
    
                      
    val kpi_measure_department_map= sqlContext.sql(
        "SELECT "+
        "DISTINCT "+
        "AB.ATTRIBUTES,DEPT.DEPARTMENT_ID,'KPI' as category "+
        "FROM "+
        "DIM_OSX_DEPARTMENT_MI DEPT "+
        "INNER JOIN PARAMETER_TBL AB "+
        "ON "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL1,'NA')||';', ';'||NVL(DEPT.L1_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'DEPARTMENT' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL2,'NA')||';', ';'||NVL(DEPT.L2_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'DEPARTMENT' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL3,'NA')||';', ';'||NVL(DEPT.L3_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'DEPARTMENT' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL4,'NA')||';', ';'||NVL(DEPT.L4_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'DEPARTMENT' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL5,'NA')||';', ';'||NVL(DEPT.L5_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'DEPARTMENT' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL6,'NA')||';', ';'||NVL(DEPT.L6_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'DEPARTMENT' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL7,'NA')||';', ';'||NVL(DEPT.L7_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'DEPARTMENT' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL8,'NA')||';', ';'||NVL(DEPT.L8_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'DEPARTMENT' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL9,'NA')||';', ';'||NVL(DEPT.L9_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'DEPARTMENT' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL10,'NA')||';', ';'||NVL(DEPT.L10_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'DEPARTMENT'"    
    )
     
    kpi_measure_department_map.coalesce(1)
                              .write
                              .mode("overwrite")
                              .format("parquet")
                              .option("compression","snappy")
                              .save(hdfs_kpi_measure_department_map)
                              
    println(load_osx_other_dim.time+" "+"KPI_MEASURE_DEPARTMENT_MAP written to hdfs path: "+ hdfs_kpi_measure_department_map)
                              
                              
    val kpi_measure_profit_centre_map= sqlContext.sql (
        "SELECT "+
        "DISTINCT "+
        "AB.ATTRIBUTES,PC.PROFIT_CENTRE_CD,'KPI' as CATEGORY "+
        "FROM "+
        "DIM_OSX_PROFIT_CENTRE_MI PC "+
        "INNER JOIN PARAMETER_TBL AB "+
        "ON "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL1,'NA')||';', ';'||NVL(PC.PROFIT_CENTRE_LEVL1_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'PROFIT_CENTER' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL2,'NA')||';', ';'||NVL(PC.PROFIT_CENTRE_LEVL2_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'PROFIT_CENTER' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL3,'NA')||';', ';'||NVL(PC.PROFIT_CENTRE_LEVL3_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'PROFIT_CENTER' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL4,'NA')||';', ';'||NVL(PC.PROFIT_CENTRE_LEVL4_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'PROFIT_CENTER' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL5,'NA')||';', ';'||NVL(PC.PROFIT_CENTRE_LEVL5_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'PROFIT_CENTER' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL6,'NA')||';', ';'||NVL(PC.PROFIT_CENTRE_LEVL6_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'PROFIT_CENTER' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL7,'NA')||';', ';'||NVL(PC.PROFIT_CENTRE_LEVL7_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'PROFIT_CENTER' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL8,'NA')||';', ';'||NVL(PC.PROFIT_CENTRE_LEVL8_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'PROFIT_CENTER' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL9,'NA')||';', ';'||NVL(PC.PROFIT_CENTRE_LEVL9_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'PROFIT_CENTER' "+
        "OR "+
        "CASE WHEN INSTR (';'||NVL(AB.LEVEL10,'NA')||';', ';'||NVL(PC.PROFIT_CENTRE_LEVL10_CODE,'NA')||';') > 0 THEN 1 "+
        "ELSE 0 END =1 AND AB.DIMENSION_TYPE = 'PROFIT_CENTER'"
    )
    
    kpi_measure_profit_centre_map.coalesce(1)
                                  .write
                                  .mode("overwrite")
                                  .format("parquet")
                                  .option("compression","snappy")
                                  .save(hdfs_kpi_measure_profit_centre_map)
                                  
    println(load_osx_other_dim.time+" "+"KPI_MEASURE_PROFIT_CENTRE_MAP written to hdfs path: "+ hdfs_kpi_measure_profit_centre_map)
                      
  }
  
}
