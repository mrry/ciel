i = 11;
j = i + 1;

foo = function (x) {
	return 100 + j + x;
};

boo = function (y, z) {
    return (z) + 2000;
};

do {
	j = j + 1;
	if (j == 42) {
		break;
	}
} while ((i + j) <= 100);

k = [1, true, false, 0, "BOO!", 1, 2, 3, 4, 5, 5, 6];

k[2] = true;

m = len(k);

ns = [];

for (yy in range(0, 2000)) {
	xx = 100;
	ns[yy] = spawn(foo, [yy]);
}

qs = [];

for (ee in range(0, 2000)) {
    qs[ee] = spawn(boo, [ns[1999 - ee], *ns[ee]]);
}

testy = function (x) { return 100 + x; } (50);

lesty = (lambda x, y : 100 + testy + y)(50, 3);

hfunc = lambda x : (lambda y : x + y);

h = hfunc(10);

z = [h(20), h(30), h(40)];

dicty = { "b\n\nad" : hfunc(100), "foo" : "bar" };

zoo = dicty["b\n\nad"](145);

starqs = [];
for (zz in range(0, 2000)) {
	ww = 3 + zz;
	starqs[zz] = (*qs[zz]) + 0;
}



return starqs;
