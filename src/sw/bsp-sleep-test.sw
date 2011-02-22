include "mapreduce";

function sleep_step(l, t, n) {
        foo = exec("stdinout", {"inputs":[], "command_line":["/bin/sleep", t]}, 1);
        ret = [];
        for (i in range(n)) {
             ret[i] = 9;
        }
        return ret;
}



n_workers = int(env["N"]);
time = int(env["T"]);
k = int(env["K"]);


init = [];
for (i in range(n_workers)) {
        init[i] = 0;
}

iter = 0;

while (iter < k) {

        rets = [];

        for (i in range(n_workers)) {
                rets += spawn(sleep_step, [init, time, n_workers]);
        }

        starrets = map(lambda x: *x, rets);

        init = shuffle(starrets, n_workers);

        iter = iter + 1;

}

return 42;
