i = 11;
j = i + 1;

foo = function (x) {
	return 100 + j + x;
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

for (yy in range(0, 200)) {
	//xx = 100;
	ns[yy] = spawn(foo, [yy]);
}

testy = function (x) { return 100 + x; } (50);

lesty = (lambda x, y : 100 + testy + y)(50, 3);

hfunc = lambda x : (lambda y : x + y);

h = hfunc(10);

z = [h(20), h(30), h(40)];

dicty = { "b\n\nad" : hfunc(100), "foo" : "bar" };

zoo = dicty["b\n\nad"](145);

starns = [];
for (zz in range(0, 200)) {
	ww = 3 + zz;
	starns[zz] = *ns[199 - zz];
}

return starns;
