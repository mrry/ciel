function a(i) {
    foo = 400 + i;
    return foo;
}

function b(j) {
    bar = 500 + *j;
    return bar;
}

aspawn = spawn(a, [1]);
bspawn = spawn(b, [aspawn]);
return (*aspawn) + (*bspawn);