aref = ref("file:///tmp/atempfile");

anotheref = ref("file:///tmp/atempfile2");

if ((*aref)[*anotheref]) {
   x = 100;
} else {
   x = 555;
}

boo = exec("stdinout", {"inputs" : [anotheref], "command_line" : "/bin/cat"}, 1);

return *(boo[0]);
