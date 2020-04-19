# arbitrage-loop

Solution for https://priceonomics.com/jobs/puzzle/

Base on an article: https://reasonabledeviations.com/2019/03/02/currency-arbitrage-graphs/

# Complexity analysis

Negative loop detection using Bellman-Ford shortest path in graph algorithm.
The complexity of Bellman-Ford is `O(E*V)` where E is number of edges and V number of vertices.
In a case of currency exchange rates we're dealing with a complete graph which means the complexity for the algorithm with a single source is `O(n^2)`.
To find all possible loops we need to run the algorithm for each vertex which makes the whole program `O(n^3)` complex.
However there is only a finite number of currencies available so `O(n^3)` complexity is enough for the given example.

# Run

To run use either sbt with `sbt reStart` or use the ammonite repl with `amm arbitrage.sc`
