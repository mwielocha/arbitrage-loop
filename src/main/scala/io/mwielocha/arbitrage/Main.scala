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
      .runForeach(printResult)
  }
}
