
include "grab";

jar_ref = ref("file:///home/chris/skywriting/src/java/tests/JitteryProducer.jar");                 

producer_out = spawn_exec("java", {"inputs":[], "lib":[jar_ref], "class":"JitteryProducer", "argv":[], "stream_output": true}, 1);

consumer_out = spawn_exec("stdinout", {"inputs":producer_out, "command_line":["head", "-c", "100"]}, 1);

return *(consumer_out[0]);