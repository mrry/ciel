function f(x) {
    foo = print("Hello world: " + str(x));
    return true;
}

for (i in range(10)) {
    result = *spawn(f, [i]);
}

return result;