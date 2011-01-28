
include "grab";

words = ref("http://news.bbc.co.uk/");
shermans = ref("http://nytimes.com/");

boo = spawn(exec, ["stdinout", {"inputs" : [words], "command_line" : ["wc", "-w"]}, 1]);
foo = spawn(exec, ["stdinout", {"inputs" : [shermans], "command_line" : ["wc", "-w"]}, 1]);
goo = spawn(exec, ["stdinout", {"inputs" : [words, shermans], "command_line" : ["wc", "-w"]}, 1]);

return [*((*boo)[0]), *((*foo)[0]), *((*goo)[0])];
