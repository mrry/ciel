// Library functions
include "grab";
//include "java";

// Numbers of blocks
num_rows = 10;
num_cols = 10;

// Java code
java_lib = [grab("http://www.cl.cam.ac.uk/~dgm36/dp.jar")];

// Input data - MODIFY THIS FOR EACH RUN
// Use sw-load to put http://www.cl.cam.ac.uk/~dgm36/horizontal_string_random
// and http://www.cl.cam.ac.uk/~dgm36/vertical_string_random into the cluster
// and paste references returned into here
horiz_source = *ref("swbs://lochain-0.xeno.cl.cam.ac.uk:8001/upload:c9cfd34b-26b2-4497-a0c5-c0ea50cbf436:index");
vert_source = *ref("swbs://lochain-0.xeno.cl.cam.ac.uk:8001/upload:853bbb00-fc8a-44cc-8387-7da01e631385:index");

// -----------------------------------------

horiz_chunks = java("tests.dp.PartitionInputString", horiz_source, [], java_lib, num_cols);
vert_chunks = java("tests.dp.PartitionInputString", vert_source, [], java_lib, num_rows);

blocks = [];

blocks[0] = [];

blocks[0][0] = java("tests.dp.SmithWaterman", [horiz_chunks[0], vert_chunks[0]], ["tl", -1, -1, -1, 2], java_lib, 3);
for (j in range(1, num_cols)) {
    blocks[0][j] = java("tests.dp.SmithWaterman", [horiz_chunks[j], vert_chunks[0], blocks[0][j-1][2]], ["t", -1, -1, -1, 2], java_lib, 3);
}

i = 0;
j = 0;

for (i in range(1, num_rows)) {
    
    blocks[i] = [];

    blocks[i][0] = java("tests.dp.SmithWaterman", [horiz_chunks[0], vert_chunks[i], blocks[i-1][0][1]], ["l", -1, -1, -1, 2], java_lib, 3);
    
    for (j in range(1, num_cols)) {
       blocks[i][j] = java("tests.dp.SmithWaterman", [horiz_chunks[j], vert_chunks[i], blocks[i-1][j-1][0], blocks[i-1][j][1], blocks[i][j-1][2]], ["i", -1, -1, -1, 2], java_lib, 3);
    }

}

// -----------------------------------------

ignore = spawn_exec("sync", {"inputs":[blocks[i][j][0]]}, 1);

return *(ignore[0]);
