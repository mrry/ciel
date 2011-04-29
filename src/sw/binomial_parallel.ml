open Printf

let init_vals t v rf n =
  let h = t /. (float_of_int n) in
  let xd = (rf -. 0.5 *. (v ** 2.)) *. h in
  let xv = v *. (sqrt h) in
  let u = exp ( xd +. xv ) in
  let d = exp ( xd -. xv ) in
  let drift = exp (rf *. h) in
  let q = (drift -. d) /. (u -. d) in
  q, u, d, drift

let c_stkval n s u d j =
  s *. (u ** (n-.j)) *. (d ** j)

let gen_initial_optvals n s u d k cp =
  for j = n downto 0 do
    let stkval = c_stkval (float n) s u d (float j) in
    output_value stdout (max 0. (cp *. (stkval -. k)))
  done

let eqn q drift a b =
  ((q *. a) +. (1.0 -. q) *. b) /. drift

let apply_column v acc pos chunk q drift = 
  let v' = ref acc.(0) in
  acc.(0) <- v;
  let maxcol = min chunk pos in
  for idx = 1 to maxcol do
    let nv' = eqn q drift acc.(idx-1) !v' in
    v' := acc.(idx);
    acc.(idx) <- nv';
  done;
  if maxcol = chunk then output_value stdout acc.(maxcol)

let process_rows rowstart rowto q drift =
  output_value stdout rowto;
  let chunk = rowstart - rowto in
  let acc = Array.create (chunk+1) 0.0 in
  for pos = 0 to rowstart do
    let v = input_value stdin in
    apply_column v acc pos chunk q drift;
  done

let _ = 
  let float_arg x = float_of_string (Sys.argv.(x)) in
  let int_arg x = int_of_string (Sys.argv.(x)) in
  let [s;k;t;v;rf;cp] = List.map float_arg [1;2;3;4;5;6] in
  let [n;chunk;start] = List.map int_arg [7;8;9] in
  let q,u,d,drift = init_vals t v rf n in
  (match start with 
  |1 -> 
      output_value stdout n;
      gen_initial_optvals n s u d k cp
  |_ ->
      let rowstart = input_value stdin in
      if rowstart = 0 then
        printf "%.9f\n%!" (input_value stdin)
      else 
        let rowto = max 0 (rowstart - chunk) in
        process_rows rowstart rowto q drift
  ); flush stdout; flush stderr
