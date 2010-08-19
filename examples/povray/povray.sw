script = "/home/avsm/src/github/avsm/skywriting/examples/povray/";
runonce = script + "run_one.py";
stitch = script + "concat.py";

gridx = 300;
gridy = 300;
sz = 100;

function gridrun() {
    res = [];
    for (x in range(0, gridx, sz)) {
      for (y in range(0, gridy, sz)) {
         f = spawn_exec("environ", {"inputs" : [], "command_line" : [runonce, gridx, gridy, x, y, sz]}, 1);
         res += f[0];
      }
    }
    return res;
}

function gridstitch(grid) {
    return spawn_exec("environ", {"inputs": grid, "command_line" : [stitch, gridx, gridy, sz]}, 1);
}

return (gridstitch(gridrun()));
