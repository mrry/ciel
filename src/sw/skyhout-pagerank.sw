include "grab";
include "mapreduce";
include "java";

jar_lib = [grab("http://www.cl.cam.ac.uk/~dgm36/skyhout.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/mahout-core-0.3.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/mahout-math-0.3.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/mahout-collections-0.3.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/commons-logging-1.1.1.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/slf4j-api-1.5.8.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/slf4j-jcl-1.5.8.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/uncommons-maths-1.2.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/gson-1.3.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/hadoop-core-0.20.2.jar")];

input = *grab(env["GRAPH_INDEX_URL"]);

num_partitions = 10;

links = mapreduce(input, lambda x: java("skywriting.examples.skyhout.pagerank.PageRankPartitionTask", [x], [], jar_lib, num_partitions),
         	              lambda xs: java("skywriting.examples.skyhout.pagerank.PageRankPartitionMergeTask", xs, [], jar_lib, 1)[0], num_partitions);

scores = mapreduce(input, lambda x: java("skywriting.examples.skyhout.pagerank.PageRankInitTask", [x], [], jar_lib, num_partitions),
         	              lambda xs: java("skywriting.examples.skyhout.pagerank.PageRankInitMergeTask", xs, [], jar_lib, 1)[0], num_partitions);

function zip(x, y) {
	z = [];
	for (i in range(len(x))) {
		z[i] = [x[i], y[i]];
	}
	return z;
}

for (i in range(10)) {
	link_scores = zip(links, scores);
	scores = mapreduce(link_scores, lambda x: java("skywriting.examples.skyhout.pagerank.PageRankZipTask", x, [], jar_lib, num_partitions),
	                                lambda xs: java("skywriting.examples.skyhout.pagerank.PageRankReduceTask", xs, [], jar_lib, num_partitions)[0], num_partitions);
}

return *(spawn_exec("sync", {"inputs" : scores}, 1)[0]);
