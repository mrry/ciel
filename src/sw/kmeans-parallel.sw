
include "grab";

java_lib = [ref("http://www.cl.cam.ac.uk/~dgm36/kmeans.jar")];
num_partitions = 10;

points = [];
for (i in range(0, num_partitions)){ 
    points = points + [spawn_exec("java", {"inputs":[], "lib":java_lib, "class":"tests.kmeans.KMeansInitialPoints", "argv":[100000, 10, "g", 2]}, 1)[0]];
}

new_centroids = spawn_exec("java", {"inputs":[], "lib":java_lib, "class":"tests.kmeans.KMeansInitialPoints", "argv":[20, 10, "u"]}, 1)[0];

do {
    
    centroids = new_centroids;
    map_output = [];
    for (partition in points) {
	map_output = map_output + [spawn_exec("java", {"inputs":[partition, centroids], "lib":java_lib, "class":"tests.kmeans.KMeansMap", "argv":[10, 10]}, 1)[0]];
    }
    new_centroids = spawn_exec("java", {"inputs":map_output, "lib":java_lib, "class":"tests.kmeans.KMeansReduce", "argv":[10, 10]}, 1)[0];
    convergence_test_output = spawn_exec("java", {"inputs":[centroids, new_centroids], "lib":java_lib, "class":"tests.kmeans.KMeansComputeError", "argv":["10", "0.00001"]}, 1)[0];
    is_converged = *convergence_test_output;
} while (!is_converged);

return new_centroids;