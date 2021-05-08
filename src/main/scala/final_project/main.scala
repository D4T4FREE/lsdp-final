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

  def Israeli_Itai(g_in: Graph[Int,Int]): List[(Int,Int)] = {
    var g = g_in
    var g_out : List[(Int, Int)] = List()
    var remaining_vertices = 2
    var r = scala.util.Random

    while(remaining_vertices >= 1) {
      // Step 1: propose to a neighbor (send your id to neighbors and they will filter out 1)
      val v1:VertexRDD[Int] = g.aggregateMessages[Int](
        triplet => {
          if (triplet.dstAttr != -1) {
            triplet.sendToDst(triplet.srcId.toInt)
          }
        },
        (a,b) => if (r.nextInt%2 == 1) a else b
      )
      val g1 = g.joinVertices(v1)(
        (uid, oldattr, proposed_id) => if (oldattr == -1) oldattr else proposed_id
      )

      // Step 2: filter out the proposed_id
      val v2:VertexRDD[Int] = g1.aggregateMessages[Int](
        triplet => {
          if (triplet.dstAttr != -1 && triplet.dstAttr == triplet.srcId.toInt) {
            triplet.sendToDst(0)
            triplet.sendToSrc(triplet.dstId.toInt)
          }
        },
        (a,b) => if (r.nextInt%2 == 1) a else b
      )
      val g2 = g1.joinVertices(v2)(
        (uid, oldattr, filtered_id) => if (oldattr == -1) oldattr else filtered_id
      )

      // Step 3: generate 0 and 1 for each vertex
      val v3:VertexRDD[Int] = g2.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr != -1) {
            triplet.sendToSrc(r.nextInt%2)
          }
          if (triplet.dstAttr != -1) {
            triplet.sendToDst(r.nextInt%2)
          }
        },
        (a,b) => (a + b)%2
      )
      val g3 = g2.joinVertices(v3)(
        (uid, oldattr, onezero) => if (oldattr == -1) oldattr else onezero*oldattr
      )

      // Step 4: figure out which proposals worked
      val v4:VertexRDD[Int] = g3.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == triplet.dstId.toInt && triplet.dstAttr != -1) {
            println(triplet.srcId + "," + triplet.dstId)
            triplet.sendToDst(-1)
            triplet.sendToSrc(-1)
          }
        },
        (a,b) => Math.min(a,b)
      )
      val g4 = g3.joinVertices(v4)(
        (uid, oldattr, finished) => if (oldattr == -1 || finished == -1) -1 else 0
      )
      g = g4
      g.cache()

      remaining_vertices = g.triplets.filter({case triplet => (triplet.srcAttr != -1) && (triplet.dstAttr != -1)}).count().toInt
    }
    return g_out
  }

  // g_in : Graph G
  // m_in : matching M on G
  // def find_augmenting_path(g_in: Graph[Int,Int], m_in:Graph[Int,Int]):List[(Int,Int)]={ //path is a list of tuples
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
  // }
  // def find_maximum_matching(g_in:Graph[Int,Int], m_in:Graph[Int,Int]):Graph[Int,Int]{
    // val aug_path = find_augmenting_path(g_in,m_in)
    // if(len(aug_path) > 0){
      // return find_maximum_matching(g_in, augment(aug_path))
    // }
    // else
      // return m_in
    // }
  // }

  // def augment(m_in: Graph[Int,Int], p_in: List[(Int,Int)]): Graph[Int,Int]={
    //var g=m_in
    //var p=p_in
    //for(i<- 1 to p.length){
      //val a=p(i)
      //val b=p(i+1)
      //val v1: VertexRDD[Int]=g.aggregateMessages[Int](
          //triplet=>{
            //if(triplet.srcId==a._2 && triplet.dstId==b._1){ //flip the path
              //triplet.sendToSrc(5)
              //triplet.sendToDst(5)
            //}
          //},
        //(a,b)=>Math.max(a,b)
        //)
    //}

    //val g1=g.joinVertices(v1)(
      //(id,mark)=>mark
      //)

    //val v2=g1.vertices.filter({case(id,x)=>(x==5)})
    //val g2=g1.joinVertices(v2)(
      //(id,old,new1)=>new1
      //)

    //g=g2
    //g.cache()

    //return g
    //}


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
      var g2 = Israeli_Itai(g) //change this

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Matching algorithm completed in " + durationSeconds + "s.") //change this
      println("==================================")

      println("Answer is: " + g2)
      //val g2df = spark.createDataFrame(g2.edges)
      //g2df.write.format("csv").mode("overwrite").save(args(2))
   }
    else{
      println("Usage: final_project graph_path output_path")
      sys.exit(1)
    }
  }
}
