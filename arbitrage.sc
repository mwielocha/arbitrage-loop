import $ivy.`com.typesafe.akka::akka-stream:2.6.4`, akka.stream.scaladsl.Source
import $ivy.`com.typesafe.akka::akka-http:10.1.11`, akka.http.scaladsl.Http, akka.http.scaladsl.model.HttpRequest, akka.http.scaladsl.unmarshalling.Unmarshal
import $ivy.`de.heikoseeberger::akka-http-circe:1.31.0`, de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._

import java.time.LocalDateTime
import akka.actor.ActorSystem
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Success

import scala.annotation.tailrec

final case class Vertex(id: String) extends AnyVal
final case class Path(previous: Vertex, distance: Double)
final case class Edge(source: Vertex, target: Vertex, weight: Double)


/*
 * Negative loop detection using Bellman-Ford shortest path in graph algorithm.
 * The complexity of Bellman-Ford is O(E*V) where E is number of edges and V number of vertices.
 * In a case of currency exchange rates we're dealing with a complete graph which means the complexity
 * for the algorithm with a single source is O(n^2).
 * To find all possible loops we need to run the algorithm for each vertex which makes the whole
 * program O(n^3) complex.
 * However there is only a finite number of currencies available so O(n^3) complexity is enough for the given example.
 */
object Arbitrage {

  def find(from: Vertex, graph: Seq[Edge]): Set[Seq[Vertex]] = {
    val normalized = graph.map(e => e.copy(weight = -1d * math.log(e.weight)))
    val distances = bellmanFord(from, normalized)
    findLoops(from, normalized, distances)
  }

  def findLoops(from: Vertex, graph: Seq[Edge], paths: Map[Vertex, Path]): Set[Seq[Vertex]] =
    graph.foldLeft(Set.empty[Seq[Vertex]]) {
      case (cycles, edge) =>

        @tailrec
        def retrace(v: Vertex, cycle: Seq[Vertex]): Seq[Vertex] = {
          val p = paths(v).previous
          if(!cycle.contains(p)) retrace(p, cycle :+ p)
          else cycle.dropWhile(_ != p) :+ p
        }

        (for {
          sourcePath <- paths.get(edge.source)
          targetPath <- paths.get(edge.target)
          if sourcePath.distance + edge.weight < targetPath.distance
          cycle = retrace(from, Seq(from))
        } yield cycles + cycle).getOrElse(cycles)

    }


  def bellmanFord(from: Vertex, graph: Seq[Edge]): Map[Vertex, Path] = {

    val vertices: Set[Vertex] =
      graph.map(e => Set(e.source, e.target))
        .reduce(_ ++ _)

    val init = Map(from -> Path(from, 0d))
    (0 until vertices.size - 1).foldLeft(init) {
      case (paths, _) =>
        graph.foldLeft(paths) {
          case (paths, Edge(source, target, weight)) =>
            paths.get(source) match {
              case None => paths
              case Some(Path(_, distance)) =>
                val total = distance + weight
                if (paths.get(target).forall(_.distance > total)) {
                  paths.updated(target, Path(source, total))
                } else paths
            }
        }

    }
  }
}

object Main {

  def parse(in: Map[String, String]): Seq[Edge] =
    in.view.map {
      case (k, v) =>
        val h +: t +: Nil = k.split('_').toSeq
        Edge(Vertex(h), Vertex(t), v.toDouble)
    }.to(Seq)

  def runForAll(
    graph: Seq[Edge]
  )(implicit ec: ExecutionContext): Future[Set[Seq[Vertex]]] = {
    val vertices = graph
      .map(e => Set(e.source, e.target))
      .reduce(_ ++ _)

    for {
      found <- Future.traverse(vertices) { from =>
        Future {
          Arbitrage.find(from, graph)
        }
      }
    } yield found.flatten
  }

  def printResult(loops: Set[Seq[Vertex]]): Unit = {
    println(s"Arbitrage opportunities found in snapshot taken at ${LocalDateTime.now}")
    for(loop <- loops) {
      println(loop.map(_.id)
        .mkString(" -> "))
    }
    println("")
  }

  def main(arg: Array[String]): Unit = {

    println("Arbitrage")

    implicit val actorSystem: ActorSystem = ActorSystem()
    import actorSystem.dispatcher

    val pool = Http().cachedHostConnectionPoolHttps[Unit](host = "fx.priceonomics.com")

    val future = Source.repeat(HttpRequest(uri = "/v1/rates/") -> ())
      .throttle(1, 1 second)
      .via(pool).mapAsync[Seq[Edge]](1) {
      case (Success(response), _) =>
        Unmarshal(response)
          .to[Map[String, String]]
          .map(parse)

      case _ =>
        Future.successful(Nil)

    }.mapAsync(1)(runForAll)
      .runForeach(printResult)

    Await.result(future, Duration.Inf)
  }
}

Main.main(Array.empty)