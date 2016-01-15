// edge file: distinct (u v) pairs
val edgeFile = sc.textFile("s3://silunwangbucket/distinct")
// (u, (v1, v2, v3))
val links = edgeFile.map(line => (line.split(" ")(0), line.split(" ")(1))).groupByKey()
// (u, num_of_followers)
val followers = sc.textFile("s3://silunwangbucket/task2")
// (u, rank_score) for every user
var ranks = followers.map(line => (line.split("\t")(0), 1.0))
// (u, num_of_followee) for every user
val followees = edgeFile.map(line => line.split(" ")).flatMap(edge => List((edge(0), 1), (edge(1), 0))).reduceByKey(_ + _)
// (u, r) users who do not follow anyone
val damplingUsers = followees.filter(line => line._2 == 0)
// (u, r) users who nobody follows
val emptyUsers = followers.map(line => (line.split("\t")(0), line.split("\t")(1))).filter(line => line._2 == "0")

val ITERATIONS = 10
val NUM_USER = 2546953
var sum = 0.0

for (i <- 1 to ITERATIONS) {
  val contribs = links.join(ranks).flatMap {
    case (id, (neighbors, rank)) => neighbors.map(benefitor => (benefitor, rank/neighbors.size))
  }
  // (u, rank)
  val emptyContribs = damplingUsers.join(ranks)
  // sum of dampling user weights
  sum = emptyContribs.values.values.sum()
  // update rank scores: benefitors + emptyUsers (no benefits)
  ranks = contribs.reduceByKey(_ + _).map(p => (p._1, 0.15 + 0.85 * (p._2 + sum / NUM_USER))) ++ emptyUsers.map(p => (p._1, 0.15 + 0.85 * sum / NUM_USER))
}

ranks.map(line => line._1 + "\t" + line._2.toString).saveAsTextFile("/target")
