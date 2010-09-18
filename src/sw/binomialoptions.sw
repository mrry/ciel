include "stdinout";

function jarrow_rudd(s, k, t, v, rf, cp, n, chunk) {
    // s: initial stock price
    // k: strike price
    // t: expiration time
    // v: volatility
    // rf: risk-free rate
    // cp: +1/-1 for call/put
    // n: number of steps */
    // chunks: number of rows per task
    cmd1 = [ "src/sw/binomial-parallel", s, k, t, v, rf, cp, n, chunk, "1"];
    cmd2 = [ "src/sw/binomial-parallel", s, k, t, v, rf, cp, n, chunk, "0"];
    r = stdinout_stream([], cmd1);
    for (i in range(0, n, chunk)) {
      r = stdinout_stream([r], cmd2);
    }
    return *stdinout([r], cmd2);
}

return jarrow_rudd(100, 100, 1, "0.3", "0.03", "-1", 100, 10 );
