 
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkPi { 
	
	def dijkstra[VD](g:Graph[VD,Double], origin:VertexId) = {
		var g2 = g.mapVertices(
			(vid,vd) => (false, if (vid == origin) 0 else Double.MaxValue))
		for (i <- 1L to g.vertices.count-1) {
			val currentVertexId =
				g2.vertices.filter(!_._2._1)
					.fold((0L,(false,Double.MaxValue)))((a,b) =>
						if (a._2._2 < b._2._2) a else b)
					._1
			//println(currentVertexId)
			val newDistances = g2.aggregateMessages[Double](
				ctx => if (ctx.srcId == currentVertexId)
					ctx.sendToDst(ctx.srcAttr._2 + ctx.attr),
				(a,b) => math.min(a,b))
			g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) =>
				(vd._1 || vid == currentVertexId,
					math.min(vd._2, newSum.getOrElse(Double.MaxValue))))
			print("-----OJO:")
			g2.vertices.collect.foreach(println(_))
		}
		g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
			(vd, dist.getOrElse((false,Double.MaxValue))._2))
	}

	def aStar[VD,ED](g:Graph[VD,ED], origin:VertexId, end:VertexId, size:Int) = {
		var g3 = g.mapVertices(
                        (vid,vd) => (false, if (vid == origin) math.abs(vid/size-end/size)+math.abs(vid%size-end%size) else Double.MaxValue, Long.MaxValue))
                //g3.vertices.collect.foreach(println(_))
		
		var currentVertexId = origin
		while(currentVertexId!=end){
			currentVertexId =
                                g3.vertices.filter(!_._2._1)
                                        .fold((0L,(false,Double.MaxValue, Long.MaxValue)))((a,b) =>
                                                if (a._2._2 < b._2._2) a else b)
                                        ._1
			val newDistances = g3.aggregateMessages[(Double, VertexId)](
                                ctx => if (ctx.srcId == currentVertexId)
                                        ctx.sendToDst(((math.abs(ctx.dstId/size-origin/size) + math.abs(ctx.dstId%size-origin%size))*1 + (math.abs(ctx.dstId/size-end/size) + math.abs(ctx.dstId%size-end%size))*10, ctx.srcId)),
                                (a,b) => if (a._1 < b._1) a else b)
			//val newParents = g3.aggregateMessages[Double](
                        //        ctx => if (ctx.srcId == currentVertexId)
                        //                ctx.sendToDst(ctx.srcId))
			g3 = g3.outerJoinVertices(newDistances)((vid, vd, newSum) =>{
				val newSumVal = newSum.getOrElse((Double.MaxValue,Long.MaxValue))
                                (vd._1 || vid == currentVertexId,
                                        math.min(vd._2, newSumVal._1), math.min(vd._3, newSumVal._2))})
			
			//g3 = g3.outerJoinVertices(newParents)((vid, vd, newParent) =>
                        //        (vd._1, vd._2, newParent.get))
			
                        //print("-----OJO:")
                        //g3.vertices.collect.foreach(println(_))
			
			//currentVertexId = end
		}
	
	}

	def time[R](block: => R): R = {
    		val t0 = System.nanoTime()
    		val result = block    // call-by-name
    		val t1 = System.nanoTime()
    		println("\n\nElapsed time: " + (t1 - t0) + "ns\n\n")
    		result
	}

	def toGexf[VD,ED](g:Graph[VD,ED]) =
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
		"<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
		" <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
		"	<nodes>\n" +
		g.vertices.map(v => "	<node id=\"" + v._1 + "\" label=\"" +
			v._2 + "\" />\n").collect.mkString +
		"	</nodes>\n" +
		"	<edges>\n" +
		g.edges.map(e => "	<edge source=\"" + e.srcId +
			"\" target=\"" + e.dstId + "\" label=\"" + e.attr +
			"\" />\n").collect.mkString +
		"	</edges>\n" +
		" </graph>\n" +
		"</gexf>"		

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Spark Pi")
		val sc = new SparkContext(conf)
		val myGraph = util.GraphGenerators.gridGraph(sc, 20, 20)
		println("A star:")
		time{aStar(myGraph, 0, 399, 20)}
		println("lib:")
		//time{lib.ShortestPaths.run(myGraph,Array(399)).vertices.collect}
		
		val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
					(4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
		val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
				Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
				Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
				Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
		val myGraph1 = Graph(myVertices, myEdges)
		//dijkstra(myGraph1, 1L).vertices.map(_._2).collect.foreach(println(_))
		val pw = new java.io.PrintWriter("myGraph.gexf")
		pw.write(toGexf(myGraph))
		pw.close
		println("Done Dijkstra")
		sc.stop()
	}
}

