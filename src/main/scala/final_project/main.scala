package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  
  
   def Israeli_Itai(g_in: Graph[Int,Int]): Graph[Int,Int] = {
    var g=g_in
    var remaining_vertices=2
   
    while(remaining_vertices>=1){
      var r=scala.util.Random
      val v1:VertexRDD[(Int,Int)]=g.aggregateMessages[(Int,Int)](
          triplet=>{
            if(triplet.dstAttr!=1){
              triplet.sendToDst((triplet.srcId.toInt,1))//send (vertexID,1) to neighbors
            }
          },
        (a,b)=> (if(r.nextFloat<a._2/(a._2+b._2)) (a._1,a._2+b._2) else b)//randomly choose one proposal
        )
      
      val g1=g.joinVertices(v1)(
          (id, id_num,mark)=>id_num)
      
      val v2:VertexRDD[Int]=g1.aggregateMessages[Int](
        triplet=>{
          if(triplet.srcId==triplet.dstAttr){
            triplet.sendToDst(r.nextInt%2)//randomly generate 0 or 1
            triplet.sendToSrc(r.nextInt%2)
          }
        },
        (a,b)=>a+b
        )
      
      val g2=g1.joinVertices(v2)(
          (id,old,new1)=>new1)
      
      val v3:VertexRDD[Int]=g2.aggregateMessages[Int](
        triplet=>{
          if(triplet.srcAttr==0 && triplet.dstAttr==1){
            triplet.sendToDst(5)
            triplet.sendToSrc(5)
          }
        },
        (a,b)=>Math.min(a,b)
        )
        
      val g3=g2.joinVertices(v3)(
          (id,old,new2)=>new2)
      
      g=g3
      g.cache()
      
      remaining_vertices=g3.vertices.count().toInt
    }
    //use from edges
    return g
  }
  
  def find_augmenting_path(g_in: Graph[Int,Int], m_in:Graph[Int,Int]):Graph[Int,Int]={
    //blossom
  }
  
  
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("final_project")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    
    
    if(args.length==0 || args.length>2){
      println("Usage: final_project graph_path output_path")
      sys.exit(1)
    }
    
    if(args.length==2){
    
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = Israeli_Itai(g)//change this

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")//change this
      println("==================================")
    
      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
   }
    else{
      println("Usage:Final project")
      sys.exit(1)
    }
  }
}
