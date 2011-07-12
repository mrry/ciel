function fib (n) {
    if (n <= 1) {
        return n;
    } else {
        x = spawn(fib, [n - 1]);
        y = spawn(fib, [n - 2]);
        return *x + *y;
    }
}    

// Attempt to read parameter from the environment.
i = int(get_key(env, "I", 10)); 

return fib(i);
