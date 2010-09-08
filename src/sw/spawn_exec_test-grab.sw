
function grab(url) {
	 return *(exec("grab", {"urls":[url], "version":0}, 1)[0]);
}

input_url = "file:///usr/share/dict/words";



inputs = [grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url)];
          
                    
test1_out = spawn_exec("stdinout", {"inputs":inputs, "command_line":["wc", "-w"]}, 1);
test2_out = spawn_exec("stdinout", {"inputs":inputs, "command_line":["wc", "-w"]}, 1);

master_jar = grab("file:///local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/tests/Master.jar");
worker_jar = grab("file:///local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/tests/Worker.jar");

test3_out = spawn_exec("stdinout", {"inputs":inputs, "command_line":["wc", "-w"]}, 1);

return (*(test3_out[0])) + (*(test2_out[0]));