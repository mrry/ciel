
function jarrow_rudd(s, k, t, v, rf, cp, n, chunk) {
    // s: initial stock price
    // k: strike price
    // t: expiration time
    // v: volatility
    // rf: risk-free rate
    // cp: +1/-1 for call/put
    // n: number of steps */
    // chunks: number of rows per task
    // command = "/opt/skywriting/src/c/tests/binomial-fc"
    command = "/local/scratch/cs448/skywriting/src/c/tests/binomial-fc";

    r = spawn_other("proc", {"command": command, "proc_pargs": [s, k, t, v, rf, cp, n, chunk, "true"], "force_n_outputs": 1});
    for (i in range(0, int(n), int(chunk))) {
      r = spawn_other("proc", {"command": command, "proc_pargs": [s, k, t, v, rf, cp, n, chunk, "false"], "force_n_outputs": 1, "extra_dependencies": [r], "proc_kwargs": {"input_ref": r}});
    }
    return *spawn_other("proc", {"command": command, "proc_pargs": [s, k, t, v, rf, cp, n, chunk, "false"], "force_n_outputs": 1, "extra_dependencies": [r], "proc_kwargs": {"input_ref": r}});
}

cs = env["CHUNKSIZE"];
tot = env["TOTAL"];
return jarrow_rudd("100", "100", "1", "0.3", "0.03", "-1", tot, cs );

