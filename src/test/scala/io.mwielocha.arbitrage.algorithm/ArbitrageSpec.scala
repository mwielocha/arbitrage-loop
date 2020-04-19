package io.mwielocha.arbitrage.algorithm

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ArbitrageSpec extends AnyFlatSpec with Matchers {

  "Bellman-ford algorithm" should "find a shortest path" in {

    val graph = Seq(
      Edge(Vertex("S"), Vertex("A"), 10),
      Edge(Vertex("S"), Vertex("E"),  8),
      Edge(Vertex("E"), Vertex("D"),  1),
      Edge(Vertex("D"), Vertex("C"), -1),
      Edge(Vertex("C"), Vertex("B"), -2),
      Edge(Vertex("B"), Vertex("A"),  1),
      Edge(Vertex("A"), Vertex("C"),  2),
      Edge(Vertex("D"), Vertex("A"), -4),
    )

    Arbitrage.bellmanFord(Vertex("S"), graph).view.mapValues(_.distance).toMap shouldBe Map(
      Vertex("S") -> 0d,
      Vertex("A") -> 5d,
      Vertex("B") -> 5d,
      Vertex("C") -> 7d,
      Vertex("D") -> 9d,
      Vertex("E") -> 8d,
    )

  }

  it should "find an arbitrage loop" in {

    val graph = Seq(
      Edge(Vertex("USD"), Vertex("EUR"), 0.7779),
      Edge(Vertex("USD"), Vertex("JPY"), 102.4590),
      Edge(Vertex("USD"), Vertex("BTC"), 0.0083),

      Edge(Vertex("EUR"), Vertex("USD"), 1.2851),
      Edge(Vertex("EUR"), Vertex("JPY"), 131.7110),
      Edge(Vertex("EUR"), Vertex("BTC"), 0.01125),

      Edge(Vertex("JPY"), Vertex("EUR"), 0.0075),
      Edge(Vertex("JPY"), Vertex("BTC"), 0.0000811),

      Edge(Vertex("BTC"), Vertex("USD"), 115.65),
      Edge(Vertex("BTC"), Vertex("EUR"), 88.8499),
      Edge(Vertex("BTC"), Vertex("JPY"), 12325.44),
    )

    Arbitrage.find(Vertex("USD"), graph) shouldBe Set(List(Vertex("BTC"), Vertex("EUR"), Vertex("JPY"), Vertex("BTC")))
  }
}
