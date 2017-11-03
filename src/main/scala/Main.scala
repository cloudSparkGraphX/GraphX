
import java.io.PrintWriter
import java.util.concurrent.atomic.AtomicLong

import com.mongodb.spark.MongoSpark
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
/**
  * Created by Shifang on 2017/10/22.
  */
object Main {
  def main(args: Array[String]): Unit = {

    /* Create the SparkSession.
     * If config arguments are passed from the command line using --conf,
     * parse args for the values to set.
     */

//    val spark = SparkSession.builder()
//      .master("local")
//      .appName("MongoSparkConnectorIntro")
//      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/myNewDB.myNewCollection1")
//      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/myNewDB.myNewCollection1")
//      .getOrCreate()
//
//    val rdd = MongoSpark.load(spark)
//    println("####################     count:"+rdd.count)
//    println("####################     first:"+rdd.first)


    val sparkConf = new SparkConf().setAppName("myGraphPractice").setMaster("local")
    val sc = new SparkContext(sparkConf)

    //下面定义不同类型的顶点,以区分User和Group的 不同属性
    trait VertexProperty extends Object{
      def getVertexID : Long
      def getType : String
    }
    class UserProperty(id:Long, username:String) extends VertexProperty with Serializable{
      def getVertexID : Long ={ this.id}

      override def getType: String = { "USER"}

      def getUsername : String ={this.username}
    }
    class GroupProperty(id:Long, name:String, tags:List[String]) extends VertexProperty with Serializable{
      def getVertexID : Long ={this.id}

      override def getType: String = { "GROUP"}

      def getName : String = { this.name}

      def getTags : List[String] = { this.tags}

    }

//    //从HDFS拿数据
//    val users:RDD[String] = sc.textFile("hdfs://101.132.146.19:9000/user/root/testGraph/users.txt",5)
//    val groups:RDD[String] = sc.textFile("hdfs://101.132.146.19:9000/user/root/testGraph/groups.txt",2)
//    val user_to_group:RDD[String] = sc.textFile("hdfs://101.132.146.19:9000/user/root/testGraph/user_to_group.txt",5)

    //从本地拿数据
    val users:RDD[String] = sc.textFile("d:/data/users.txt",1)
    val groups:RDD[String] = sc.textFile("d:/data/groups.txt",1)
    val user_to_group:RDD[String] = sc.textFile("d:/data/user_to_group.txt",1)

    //构造点
    val userV:RDD[(VertexId,VertexProperty)]= users.map{
      line =>
        val fields = line.split(",")
        (fields(0).toLong,new UserProperty(fields(0).toLong,fields(1)))
    }

    val groupV:RDD[(VertexId,VertexProperty)] = groups.map{
      line =>
        val fields = line.split(",")
        (fields(0).toLong,
          new GroupProperty(
            fields(0).toLong,
            fields(1),
            fields(2).split(Array(';','[',']')).toList.drop(1)))
    }

    val nodes:RDD[(VertexId,VertexProperty)] = userV ++ groupV

    //构造边
    val edges = user_to_group.map{
      line =>
        val fields = line.split(",")
        Edge(fields(0).toLong,fields(1).toLong,1L)
    }

    val userProperty:UserProperty=new UserProperty(1L,"Default User")

    //生成图
    val graph:Graph[VertexProperty,Long] = Graph(nodes,edges,userProperty).persist()

    //保存图
//    graph.vertices.saveAsTextFile("d:/data/douban_vertices")
//    graph.edges.saveAsTextFile("d:/data/douban_edges")

    //可以先做一个filter，生成展示要用的数据

//    val generateEdgeID= ()=>{
//      var idCounter = 1L;
//      return ()=>{
//        idCounter = idCounter+1;
//        return idCounter;
//      }
//    }

    var IDIncr= new AtomicLong(0L)



    val json =
       ("nodes" -> graph.vertices.collect().toList.map { node =>
         node._2 match {
           case user: UserProperty =>
             ("id" -> user.getVertexID) ~
               ("type" -> user.getType) ~
               ("username" -> user.getUsername) ~
               ("x" -> Math.random()) ~
               ("y" -> Math.random()) ~
               ("size" -> Math.random()) ~
               ("label" -> user.getUsername) ~
               ("color" -> "#FF4C4C")
           case group: GroupProperty =>
             ("id" -> group.getVertexID) ~
               ("type" -> group.getType) ~
               ("tags" -> group.getTags) ~
               ("x" -> Math.random()) ~
               ("y" -> Math.random()) ~
               ("size" -> Math.random()*3) ~
               ("label" -> group.getName) ~
               ("color" -> "#FFA64C")
         }
       }) ~
         ("edges" -> graph.edges.collect().toList.map { w =>
           ("source" -> w.srcId) ~
             ("target" -> w.dstId) ~
             ("id" -> IDIncr.incrementAndGet()) //这里手动加id
         })

   // println(pretty(render(json)))

   //写入 data.json 中
   val out = new PrintWriter("D:/GIT/graphVisualize/data/data.json")
   out.write(compact(render(json)))
   out.flush()
   out.close()

  }

}
