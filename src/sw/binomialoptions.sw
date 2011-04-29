function stdinout(input_refs, cmd_line) {
   f = int(env["FOO"]);
	return spawn_exec("stdinout", {"inputs" : input_refs, "command_line" : cmd_line, "stream_chunk_size": 1, "foo" : f }, 1)[0];
}

function stdinout_stream(input_refs, cmd_line) {
   f = int(env["FOO"]);
	return spawn_exec("stdinout", {"inputs" : input_refs, "command_line" : cmd_line, "stream_chunk_size": 1, "stream_output" : true, "foo" : f }, 1)[0];
}

function jarrow_rudd(s, k, t, v, rf, cp, n, chunk) {
    // s: initial stock price
    // k: strike price
    // t: expiration time
    // v: volatility
    // rf: risk-free rate
    // cp: +1/-1 for call/put
    // n: number of steps */
    // chunks: number of rows per task
    cmd1 = [ "binomial-ocaml", s, k, t, v, rf, cp, n, chunk, "1"];
    cmd2 = [ "binomial-ocaml", s, k, t, v, rf, cp, n, chunk, "0"];
    r = stdinout_stream([], cmd1);
    for (i in range(0, n, chunk)) {
      r = stdinout_stream([r], cmd2);
    }
    return *stdinout([r], cmd2);
}

cs = int(env["CHUNKSIZE"]);
tot = int(env["TOTAL"]);
return jarrow_rudd(100, 100, 1, "0.3", "0.03", "-1", tot, cs );

