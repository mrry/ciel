
spawnee = function() { return "hello"; };

input = spawn(spawnee, []);
                    
test1_out = exec("stdinout", {"inputs":[input], "command_line":["wc", "-c"]}, 1);

return (*(test1_out[0]));