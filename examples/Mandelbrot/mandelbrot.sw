
size_x = 2000;
size_y = 2000;
n_tiles_x = 10;
n_tiles_y = 10;
param_scale = "0.001";
param_maxit = 250;

jar_lib = [package("mandelbrot-jar")];

map_outputs = [];
for (i in range(0, n_tiles_x)) {
    for (j in range(0, n_tiles_y)) {
        map_outputs = map_outputs + spawn_exec("java", {"inputs":[], "argv":[size_x, size_y, n_tiles_x, n_tiles_y, i, j, param_maxit, param_scale], "class":"skywriting.examples.mandelbrot.Mandelbrot", "lib":jar_lib}, 1);
    }
}

reduce_output = spawn_exec("java", {"inputs":map_outputs, "argv":[n_tiles_x, n_tiles_y, size_x, size_y], "class":"skywriting.examples.mandelbrot.Stitch", "lib":jar_lib}, 1);

return *(exec("sync", {"inputs":reduce_output}, 1)[0]);

