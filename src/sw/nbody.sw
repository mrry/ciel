
include "grab";

inputs_url_ref = ref("http://www.cl.cam.ac.uk/~dgm36/nbody.txt");
coords_url_ref = ref("http://www.cl.cam.ac.uk/~dgm36/coords.txt");
jar_ref = ref("http://www.cl.cam.ac.uk/~dgm36/magic.jar");


function buildTree(points_ref, coords_ref) {
   splits = exec("java", { "inputs": [points_ref, coords_ref], "lib": [jar_ref], "argv":[], "class": "Splitter" }, 9);
   split_info = *(splits[8]);
   centroid = split_info["centroid"];
   count = split_info["count"];
   children = [];
   if (count <= 1) {
      return { "centroid": centroid, "count" : count };
   }
   for (i in range(0,4)) {
       children[i] = spawn(buildTree, [ splits[i], splits[i+4] ]);
   }
   return { "centroid": centroid, "count" : count, "children": children, "coords": coords_ref };
}

function walkTree(root_ref, tree_refs) {
   tree_length = len(tree_refs);
   last_tree = *tree_refs[tree_length - 1];
   if (last_tree["count"] == 0) {
       return exec("stdinout", {"inputs":[], "command_line": ["echo", "-n"]}, 1)[0];
   }
   if (last_tree["count"] == 1) {
      return exec("stdinout", {"inputs":[tree_refs[tree_length - 1]], "command_line": ["python", "-c", "import json; import sys; print json.dumps(json.load(sys.stdin)[\"centroid\"])"]}, 1)[0];
   }
   new_tree_refs = [];
   for (i in range(0,tree_length)) {
      new_tree_refs[i] = tree_refs[i];
   }
   child_results = [];
   for (i in range(0,len(last_tree["children"]))) {
      child_ref = last_tree["children"][i];
      new_tree_refs[tree_length] = child_ref;
      child_results[i] = *spawn(walkTree, [root_ref, new_tree_refs]);
   }
   combined_result = exec("stdinout", { "inputs": child_results, "command_line": ["cat"] }, 1);
   return combined_result[0];
}

tree_ref = spawn(buildTree, [inputs_url_ref, coords_url_ref]);
//ignore = spawn(lambda: 1, []);
//bar = select([ignore], 0);
//foo = exec("stdinout", {"inputs":[], "command_line":["/bin/sleep", "6"]}, 1);
return walkTree(tree_ref, [tree_ref]);
