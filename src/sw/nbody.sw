inputs_url = "http://localhost/~avsm/nbody.txt";
coords_url = "http://localhost/~avsm/coords.txt";
jar_ref = ref("http://localhost/~avsm/magic.jar");

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
       children[i] = *spawn(buildTree, [ splits[i], splits[i+4] ]);
   }
   return { "centroid": centroid, "count" : count, "children": children, "coords": coords_ref };
}

return buildTree(ref(inputs_url), ref(coords_url));
