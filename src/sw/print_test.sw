function f(x) {
    foo = print("Hello world: " + x);
    return true;
}

for i in (range(10)) {
    result = *spawn(f, []);
}

return result;