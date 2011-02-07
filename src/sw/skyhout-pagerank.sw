include "mapreduce";
include "java";

jar_lib = [package("skyhout"), package("mahout-core"), package("mahout-math"), package("mahout-coll"), 
	   package("logging"), package("slf4j-api"), package("slf4j-jcl"), package("uc-maths"), 
	   package("gson"), package("hadoop-core")];

input = *package("graph-in");

num_partitions = 10;

links = mapreduce(input, lambda x: java("skywriting.examples.skyhout.pagerank.PageRankInitTask", [x], [], jar_lib, num_partitions),
         	              lambda xs: java("skywriting.examples.skyhout.pagerank.PageRankInitMergeTask", xs, [], jar_lib, 1)[0], num_partitions);

scores = map(lambda x: java("skywriting.examples.skyhout.pagerank.PageRankInitialScoreTask", [x], [], jar_lib, 1)[0], links);

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

sorted_scores = mapreduce(scores, lambda x: java("skywriting.examples.skyhout.pagerank.PageRankSortMapTask", [x], [], jar_lib, 1),
	      			  lambda xs: java("skywriting.examples.skyhout.pagerank.PageRankSortReduceTask", xs, [], jar_lib, 1)[0], 1); 

return *(spawn_exec("sync", {"inputs" : sorted_scores}, 1)[0]);
