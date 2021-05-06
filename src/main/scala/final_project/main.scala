package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

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

  // g_in : Graph G
  // m_in : matching M on G
  def find_augmenting_path(g_in: Graph[Int,Int], m_in:Graph[Int,Int]):Graph[Int,Int]={
    // thank you internet for helping me come up with pseudocode
    // notes from prof: restrict length of augmenting path, restrict size of blossom

    // F <- empty forest

    // unmark all vertices and edges in G, mark all edges of match

    // for each exposed vertex v do
      // create a singleton tree { v } and add the tree to F
    //end for

    // while there is an umarked vertex v in F with distance(v, root(v)) even do
      // while there exists an unarked edge e = { v, w } do
        // if w is not in F then
          // // w is matched, so add e and w's matched edge to F
          // x <- vertex matched to w in match
          // add edges { v, w } and { w, x } to the tree of v
        // else
          // if distance(w, root(w)) is odd then
            // // Do nothing.
          // else
            // if root(v) != root(w) then
              // // Report an augmenting path in F union { e }
              // P <- path (root(v) -> ... >- v) -> (w -> ... -> root(w))
              // return P
            // else
              // // Contract a blossom in G and look for the path in the contracted graph
              // B <- blossom formed by e and edges on the path v -> w in T
              // G', M' <- contract G and M by B
              // P' <- find_augmenting_path(G', M')
              // P <- lift P' to G
              // return P
            // end if
          //end if
        // end if
        // mark edge e
      // end while
      // mark vertex v
    // end while
    // return empty path
  }
  def find_maximum_matching(g_in:Graph[Int,Int], m_in:Graph[Int,Int]):Graph[Int,Int]{
     //function find_maximum_matching(G, M) : M*
     // P ← find_augmenting_path(G, M)
  //   if P is non-empty then
 //       return find_maximum_matching(G, augment M along P)
   //  else
  //      return M
  //   end if
 // end function
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
      val g2 = Israeli_Itai(g) //change this

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Matching algorithm completed in " + durationSeconds + "s.") //change this
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
   }
    else{
      println("Usage: final_project graph_path output_path")
      sys.exit(1)
    }
  }
}
