// Library functions
include "grab";
//include "java";

function java(class_name, input_refs, argv, jar_refs, num_outputs, may_pipe) {
//   f = env["FOO"];
       	if(env["pipe_output"] == "true" && may_pipe) {
	   pipe_output = true;
	}	   
	else {
	   pipe_output = false;
	}
	return spawn_exec("java", {"inputs" : input_refs, "class" : class_name, "lib" : jar_refs, "argv" : argv, "stream_output": true, "single_consumer": may_pipe, "pipe_output": pipe_output}, num_outputs);
}  

// Numbers of blocks
num_rows = int(env["NUM_ROWS"]);
num_cols = int(env["NUM_COLS"]);

// Java code
java_lib = [package("smithwaterman_jar")];

// Input data
horiz_source = *(package("input1"));
vert_source = *(package("input2"));

// -----------------------------------------

horiz_chunks = java("skywriting.examples.smithwaterman.PartitionInputString", horiz_source, [], java_lib, num_cols, false);
vert_chunks = java("skywriting.examples.smithwaterman.PartitionInputString", vert_source, [], java_lib, num_rows, false);

blocks = [];

blocks[0] = [];

blocks[0][0] = java("skywriting.examples.smithwaterman.SmithWaterman", [horiz_chunks[0], vert_chunks[0]], ["tl", -1, -1, -1, 2], java_lib, 3, true);
for (j in range(1, num_cols)) {
    blocks[0][j] = java("skywriting.examples.smithwaterman.SmithWaterman", [horiz_chunks[j], vert_chunks[0], blocks[0][j-1][2]], ["t", -1, -1, -1, 2], java_lib, 3, true);
}

i = 0;
j = 0;

for (i in range(1, num_rows)) {
    
    blocks[i] = [];

    blocks[i][0] = java("skywriting.examples.smithwaterman.SmithWaterman", [horiz_chunks[0], vert_chunks[i], blocks[i-1][0][1]], ["l", -1, -1, -1, 2], java_lib, 3, true);
    
    for (j in range(1, num_cols)) {
       blocks[i][j] = java("skywriting.examples.smithwaterman.SmithWaterman", [horiz_chunks[j], vert_chunks[i], blocks[i-1][j-1][0], blocks[i-1][j][1], blocks[i][j-1][2]], ["i", -1, -1, -1, 2], java_lib, 3, true);
    }

}

// -----------------------------------------

return (*(spawn_exec("sync", {"inputs":[blocks[i][j][0]]}, 1)[0]))[0];
