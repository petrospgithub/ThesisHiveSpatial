import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2

/**
  * Created by ppetrou on 4/29/17.
  */

case class Record(key: Int, value: String)

object ThesisServer {
  def main(args: Array[String]): Unit = {

    val spark = args(0) match {
      case "true" => SparkSession.builder
        .appName("Spark Examples")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
      case _ => SparkSession.builder
        .appName("Spark Examples")
        //.master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    }

    val esri_jar = getClass.getResource("/jars/esri-geometry-api-1.2.1.jar")
    val hive_input_jar = getClass.getResource("/jars/spatial-sdk-hive-1.2.1-SNAPSHOT.jar")
    val hive_json_input_jar = getClass.getResource("/jars/spatial-sdk-json-1.2.1-SNAPSHOT.jar")

    import spark.implicits._

    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    // Any RDD containing case classes can be used to create a temporary view.  The schema of the
    // view is automatically inferred using scala reflection.
    df.createOrReplaceTempView("records")

    val test=HiveThriftServer2.startWithContext(spark.sqlContext)

    val result=spark.sql("SELECT * FROM records")
    result.collect().foreach(println)

    spark.sql("ADD JAR "+esri_jar)
    spark.sql("ADD JAR "+hive_input_jar)
    spark.sql("ADD JAR "+hive_json_input_jar)

    spark.sql("create temporary function ST_AsBinary as 'com.esri.hadoop.hive.ST_AsBinary'")
    spark.sql("create temporary function ST_AsGeoJSON as 'com.esri.hadoop.hive.ST_AsGeoJson'")
    spark.sql("create temporary function ST_AsJSON as 'com.esri.hadoop.hive.ST_AsJson'")
    spark.sql("create temporary function ST_AsShape as 'com.esri.hadoop.hive.ST_AsShape'")
    spark.sql("create temporary function ST_AsText as 'com.esri.hadoop.hive.ST_AsText'")
    spark.sql("create temporary function ST_GeomFromJSON as 'com.esri.hadoop.hive.ST_GeomFromJson'")
    spark.sql("create temporary function ST_GeomFromGeoJSON as 'com.esri.hadoop.hive.ST_GeomFromGeoJson'")
    spark.sql("create temporary function ST_GeomFromShape as 'com.esri.hadoop.hive.ST_GeomFromShape'")
    spark.sql("create temporary function ST_GeomFromText as 'com.esri.hadoop.hive.ST_GeomFromText'")
    spark.sql("create temporary function ST_GeomFromWKB as 'com.esri.hadoop.hive.ST_GeomFromWKB'")
    spark.sql("create temporary function ST_PointFromWKB as 'com.esri.hadoop.hive.ST_PointFromWKB'")
    spark.sql("create temporary function ST_LineFromWKB as 'com.esri.hadoop.hive.ST_LineFromWKB'")
    spark.sql("create temporary function ST_PolyFromWKB as 'com.esri.hadoop.hive.ST_PolyFromWKB'")
    spark.sql("create temporary function ST_MPointFromWKB as 'com.esri.hadoop.hive.ST_MPointFromWKB'")
    spark.sql("create temporary function ST_MLineFromWKB as 'com.esri.hadoop.hive.ST_MLineFromWKB'")
    spark.sql("create temporary function ST_MPolyFromWKB as 'com.esri.hadoop.hive.ST_MPolyFromWKB'")
    spark.sql("create temporary function ST_GeomCollection as 'com.esri.hadoop.hive.ST_GeomCollection'")

    spark.sql("create temporary function ST_GeometryType as 'com.esri.hadoop.hive.ST_GeometryType'")

    spark.sql("create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point'")
    spark.sql("create temporary function ST_PointZ as 'com.esri.hadoop.hive.ST_PointZ'")
    spark.sql("create temporary function ST_LineString as 'com.esri.hadoop.hive.ST_LineString'")
    spark.sql("create temporary function ST_Polygon as 'com.esri.hadoop.hive.ST_Polygon'")

    spark.sql("create temporary function ST_MultiPoint as 'com.esri.hadoop.hive.ST_MultiPoint'")
    spark.sql("create temporary function ST_MultiLineString as 'com.esri.hadoop.hive.ST_MultiLineString'")
    spark.sql("create temporary function ST_MultiPolygon as 'com.esri.hadoop.hive.ST_MultiPolygon'")

    spark.sql("create temporary function ST_SetSRID as 'com.esri.hadoop.hive.ST_SetSRID'")

    spark.sql("create temporary function ST_SRID as 'com.esri.hadoop.hive.ST_SRID'")
    spark.sql("create temporary function ST_IsEmpty as 'com.esri.hadoop.hive.ST_IsEmpty'")
    spark.sql("create temporary function ST_IsSimple as 'com.esri.hadoop.hive.ST_IsSimple'")
    spark.sql("create temporary function ST_Dimension as 'com.esri.hadoop.hive.ST_Dimension'")
    spark.sql("create temporary function ST_X as 'com.esri.hadoop.hive.ST_X'")
    spark.sql("create temporary function ST_Y as 'com.esri.hadoop.hive.ST_Y'")
    spark.sql("create temporary function ST_MinX as 'com.esri.hadoop.hive.ST_MinX'")
    spark.sql("create temporary function ST_MaxX as 'com.esri.hadoop.hive.ST_MaxX'")
    spark.sql("create temporary function ST_MinY as 'com.esri.hadoop.hive.ST_MinY'")
    spark.sql("create temporary function ST_MaxY as 'com.esri.hadoop.hive.ST_MaxY'")
    spark.sql("create temporary function ST_IsClosed as 'com.esri.hadoop.hive.ST_IsClosed'")
    spark.sql("create temporary function ST_IsRing as 'com.esri.hadoop.hive.ST_IsRing'")
    spark.sql("create temporary function ST_Length as 'com.esri.hadoop.hive.ST_Length'")
    spark.sql("create temporary function ST_GeodesicLengthWGS84 as 'com.esri.hadoop.hive.ST_GeodesicLengthWGS84'")
    spark.sql("create temporary function ST_Area as 'com.esri.hadoop.hive.ST_Area'")
    spark.sql("create temporary function ST_Is3D as 'com.esri.hadoop.hive.ST_Is3D'")
    spark.sql("create temporary function ST_Z as 'com.esri.hadoop.hive.ST_Z'")
    spark.sql("create temporary function ST_MinZ as 'com.esri.hadoop.hive.ST_MinZ'")
    spark.sql("create temporary function ST_MaxZ as 'com.esri.hadoop.hive.ST_MaxZ'")
    spark.sql("create temporary function ST_IsMeasured as 'com.esri.hadoop.hive.ST_IsMeasured'")
    spark.sql("create temporary function ST_M as 'com.esri.hadoop.hive.ST_M'")
    spark.sql("create temporary function ST_MinM as 'com.esri.hadoop.hive.ST_MinM'")
    spark.sql("create temporary function ST_MaxM as 'com.esri.hadoop.hive.ST_MaxM'")
    spark.sql("create temporary function ST_CoordDim as 'com.esri.hadoop.hive.ST_CoordDim'")
    spark.sql("create temporary function ST_NumPoints as 'com.esri.hadoop.hive.ST_NumPoints'")
    spark.sql("create temporary function ST_PointN as 'com.esri.hadoop.hive.ST_PointN'")
    spark.sql("create temporary function ST_StartPoint as 'com.esri.hadoop.hive.ST_StartPoint'")
    spark.sql("create temporary function ST_EndPoint as 'com.esri.hadoop.hive.ST_EndPoint'")
    spark.sql("create temporary function ST_ExteriorRing as 'com.esri.hadoop.hive.ST_ExteriorRing'")
    spark.sql("create temporary function ST_NumInteriorRing as 'com.esri.hadoop.hive.ST_NumInteriorRing'")
    spark.sql("create temporary function ST_InteriorRingN as 'com.esri.hadoop.hive.ST_InteriorRingN'")
    spark.sql("create temporary function ST_NumGeometries as 'com.esri.hadoop.hive.ST_NumGeometries'")
    spark.sql("create temporary function ST_GeometryN as 'com.esri.hadoop.hive.ST_GeometryN'")
    spark.sql("create temporary function ST_Centroid as 'com.esri.hadoop.hive.ST_Centroid'")

    spark.sql("create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains'")
    spark.sql("create temporary function ST_Crosses as 'com.esri.hadoop.hive.ST_Crosses'")
    spark.sql("create temporary function ST_Disjoint as 'com.esri.hadoop.hive.ST_Disjoint'")
    spark.sql("create temporary function ST_EnvIntersects as 'com.esri.hadoop.hive.ST_EnvIntersects'")
    spark.sql("create temporary function ST_Envelope as 'com.esri.hadoop.hive.ST_Envelope'")
    spark.sql("create temporary function ST_Equals as 'com.esri.hadoop.hive.ST_Equals'")
    spark.sql("create temporary function ST_Overlaps as 'com.esri.hadoop.hive.ST_Overlaps'")
    spark.sql("create temporary function ST_Intersects as 'com.esri.hadoop.hive.ST_Intersects'")
    spark.sql("create temporary function ST_Relate as 'com.esri.hadoop.hive.ST_Relate'")
    spark.sql("create temporary function ST_Touches as 'com.esri.hadoop.hive.ST_Touches'")

    spark.sql("create temporary function ST_Distance as 'com.esri.hadoop.hive.ST_Distance'")
    spark.sql("create temporary function ST_Boundary as 'com.esri.hadoop.hive.ST_Boundary'")
    spark.sql("create temporary function ST_Buffer as 'com.esri.hadoop.hive.ST_Buffer'")
    spark.sql("create temporary function ST_ConvexHull as 'com.esri.hadoop.hive.ST_ConvexHull'")
    spark.sql("create temporary function ST_Intersection as 'com.esri.hadoop.hive.ST_Intersection'")
    spark.sql("create temporary function ST_Union as 'com.esri.hadoop.hive.ST_Union'")
    spark.sql("create temporary function ST_Difference as 'com.esri.hadoop.hive.ST_Difference'")
    spark.sql("create temporary function ST_SymmetricDiff as 'com.esri.hadoop.hive.ST_SymmetricDiff'")
    spark.sql("create temporary function ST_SymDifference as 'com.esri.hadoop.hive.ST_SymmetricDiff'")

    spark.sql("create temporary function ST_Aggr_ConvexHull as 'com.esri.hadoop.hive.ST_Aggr_ConvexHull'")
    spark.sql("create temporary function ST_Aggr_Intersection as 'com.esri.hadoop.hive.ST_Aggr_Intersection'")
    spark.sql("create temporary function ST_Aggr_Union as 'com.esri.hadoop.hive.ST_Aggr_Union'")

    spark.sql("create temporary function ST_Bin as 'com.esri.hadoop.hive.ST_Bin'")
    spark.sql("create temporary function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope'")

    //spark.sql("CREATE TABLE demo_shape_point(shape string) STORED AS ORC")

    //spark.sql("INSERT INTO demo_shape_point VALUES ('POINT (-74.140007019999985 39.650001530000054)')")

    //spark.sql("SELECT * FROM demo_shape_point").collect().foreach(println)

    //spark.sql("SELECT ST_AsJson(ST_Polygon(1.5,2.5, 3.0,2.2, 2.2,1.1))").collect().foreach(println)

    //spark.close()
  }
}
