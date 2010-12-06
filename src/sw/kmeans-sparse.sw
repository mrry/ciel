
include "grab";

java_lib = [ref("http://www.cl.cam.ac.uk/~dgm36/kmeans.jar")];
num_partitions = 20;
num_clusters = 2;

original_file = ref("file:///local/scratch/dgm36/tmp/docword.kos.txt");

partitions = exec("java", {"inputs":[original_file], "lib":java_lib, "class":"tests.kmeans.sparse.PartitionBagOfWords", "argv":[]}, num_partitions);

new_centroids = exec("java", {"inputs":[original_file], "lib":java_lib, "class":"tests.kmeans.sparse.GenerateRandomSparse", "argv":[num_clusters]}, 1)[0];

//return new_centroids;

do {
    centroids = new_centroids;
    map_output = [];
    for (partition in partitions) {
		map_output = map_output + [spawn_exec("java", {"inputs":[partition, centroids], "lib":java_lib, "class":"tests.kmeans.sparse.KMeansMap", "argv":[num_clusters]}, 1)[0]];
    }
    new_centroids = spawn_exec("java", {"inputs":map_output, "lib":java_lib, "class":"tests.kmeans.sparse.KMeansReduce", "argv":[num_clusters]}, 1)[0];
    convergence_test_output = spawn_exec("java", {"inputs":[centroids, new_centroids], "lib":java_lib, "class":"tests.kmeans.sparse.KMeansComputeError", "argv":[num_clusters, "0.00001"]}, 1)[0];
    is_converged = *convergence_test_output;
} while (!is_converged);

return new_centroids;