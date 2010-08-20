words = ref("http://news.bbc.co.uk/");
shermans = ref("http://nytimes.com/");

function f (alpha, beta) {

    return exec("environ", {"inputs" : [alpha, beta], "command_line" : ["/home/dgm36/test.sh", 1, 2, 3]}, 3);

}

result = exec("environ", {"inputs" : [words, shermans], "command_line" : ["/home/dgm36/test.sh", 1, 2, 3]}, 3);

result = exec("environ", {"inputs" : [words, shermans], "command_line" : ["/home/dgm36/test.sh", 1, 2, 3]}, 3);

for (i in range(0, 10)) {

    result = exec("environ", {"inputs" : [words, shermans], "command_line" : ["/home/dgm36/test.sh", 1, 2, 3]}, 3);
    b = spawn(f, [words, shermans]);

}

result = spawn_exec("environ", {"inputs" : [words, shermans], "command_line" : ["/home/dgm36/test.sh", 1, 2, 3]}, 3);

r0 = *(result[0]);
r1 = *(result[1]);
r2 = *(result[2]);

return r0 + r1 + r2;
