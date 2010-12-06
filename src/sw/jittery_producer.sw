
include "grab";

jar_ref = ref("file:///home/chris/skywriting/examples/tests/src/java/JitteryProducer.jar");                 

producer_out = spawn_exec("java", {"inputs":[], "lib":[jar_ref], "class":"tests.JitteryProducer", "argv":["10"], "stream_output": true}, 2);

consumer_out = spawn_exec("stdinout", {"inputs":[producer_out[0]], "command_line":["wc", "-c"]}, 1);

return [*(consumer_out[0]), *(producer_out[1])];