function list_builder () {

    function fortytwo () {
        return 42;
    }

    length = *spawn(fortytwo, []);

    out = [];
    for (i in range(0, length)) {
        out += i;
    }

    return out;
}

list = *spawn(list_builder, []);

ret = 0;

for (i in range(0, len(list))) {
    ret += i;
}

for (i in list) {
    ret = ret - i;
}

return ret;

