function fib (n) {
    if (n <= 1) {
        return n;
    } else {
        x = spawn(fib, [n - 1]);
        y = spawn(fib, [n - 2]);
        return *x + *y;
    }
}    

return fib(10);
