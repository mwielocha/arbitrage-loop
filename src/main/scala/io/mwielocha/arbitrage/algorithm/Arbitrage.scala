package io.mwielocha.arbitrage.algorithm

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
