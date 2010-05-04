num_rows = 10;
num_cols = 10;

horiz_source = ref("http://www.cl.cam.ac.uk/~dgm36/horizontal_string_random");
vert_source = ref("http://www.cl.cam.ac.uk/~dgm36/vertical_string_random");
java_lib = [ref("http://www.cl.cam.ac.uk/~dgm36/dp.jar")];

horiz_chunks = spawn_exec("java", {"inputs":[horiz_source], "lib":java_lib, "class":"tests.dp.PartitionInputString", "argv":[]}, num_cols);
vert_chunks = spawn_exec("java", {"inputs":[vert_source], "lib":java_lib, "class":"tests.dp.PartitionInputString", "argv":[]}, num_rows);

blocks = [];

blocks[0] = [];

blocks[0][0] = spawn_exec("java", {"argv":["tl", 0-1, 0-1, 0-1, 2], "lib":java_lib, "class":"tests.dp.SmithWaterman", "inputs":[horiz_chunks[0], vert_chunks[0]]}, 3);
for (j in range(1, num_cols)) {
    blocks[0][j] = spawn_exec("java", {"argv":["t", 0-1, 0-1, 0-1, 2], "lib":java_lib, "class":"tests.dp.SmithWaterman", "inputs":[horiz_chunks[j], vert_chunks[0], blocks[0][j-1][2]]}, 3);
}

i = 0;
j = 0;

for (i in range(1, num_rows)) {
    
    blocks[i] = [];

    blocks[i][0] = spawn_exec("java", {"argv":["l", 0-1, 0-1, 0-1, 2], "lib":java_lib, "class":"tests.dp.SmithWaterman", "inputs":[horiz_chunks[0], vert_chunks[i], blocks[i-1][0][1]]}, 3);
    
    for (j in range(1, num_cols)) {
	blocks[i][j] = spawn_exec("java", {"argv":["i", 0-1, 0-1, 0-1, 2], "lib":java_lib, "class":"tests.dp.SmithWaterman", "inputs":[horiz_chunks[j], vert_chunks[i], blocks[i-1][j-1][0], blocks[i-1][j][1], blocks[i][j-1][2]]}, 3);
    }

}

ignore = exec("stdinout", {"inputs":[blocks[i][j][0]], "command_line":["cat"]}, 1);

return ignore;
