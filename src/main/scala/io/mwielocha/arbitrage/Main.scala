package io.mwielocha.arbitrage

import java.time.LocalDateTime

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.Source
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.mwielocha.arbitrage.algorithm.{Arbitrage, Edge, Vertex}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Success

object Main {

  def parse(in: Map[String, String]): Seq[Edge] =
    in.view.map {
      case (k, v) =>
        val h +: t +: Nil = k.split('_').toSeq
        Edge(Vertex(h), Vertex(t), v.toDouble)
    }.to(Seq)

  def runForAll(
    graph: Seq[Edge]
  )(implicit ec: ExecutionContext): Future[(Set[Seq[Vertex]], Seq[Edge])] = {
    val vertices = graph
      .map(e => Set(e.source, e.target))
      .reduce(_ ++ _)

    for {
      found <- Future.traverse(vertices) { from =>
        // Bellman-Ford can be easily executed in parallel
        Future {
          Arbitrage.find(from, graph)
        }
      }
    } yield found.flatten -> graph
  }

  def printResult(loops: Set[Seq[Vertex]], graph: Seq[Edge]): Unit = {

    println(s"--- Arbitrage opportunities found in snapshot taken at ${LocalDateTime.now} --- \n")
    for(loop <- loops) {

      println(loop.map(_.id)
        .mkString(" -> "))

      loop.zip(loop.tail).foldLeft(BigDecimal(1)) {
        case (product, (v1, v2)) =>
          (for(e <- graph.find(e => e.source == v1 && e.target == v2)) yield {
            val x = product * e.weight
            println(s"Exchange $product ${v1.id} to ${v2.id} for $x")
            x
          }).getOrElse(product)
      }
      println("")
    }
    println("")
  }


  def main(arg: Array[String]): Unit = {

    implicit val actorSystem: ActorSystem = ActorSystem()
    import actorSystem.dispatcher

    val pool = Http().cachedHostConnectionPoolHttps[Unit](host = "fx.priceonomics.com")

    Source.repeat(HttpRequest(uri = "/v1/rates/") -> ())
      .throttle(1, 1 second)
      .via(pool).mapAsync[Seq[Edge]](1) {
        case (Success(response), _) =>
            Unmarshal(response)
              .to[Map[String, String]]
              .map(parse)

        case _ =>
          Future.successful(Nil)

      }.mapAsync(1)(runForAll)
      .runForeach {
        case (loops, graph) =>
          printResult(loops, graph)
      }
  }
}
