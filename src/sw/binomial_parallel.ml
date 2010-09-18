open Printf
open Bigarray
open Bigarray.Array2

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

let gen_initial_optvals stkfn n k cp =
  let rec fn = function
    |(-1) -> ()
    |j -> 
       output_value stdout (max 0. (cp *. (stkfn (float j) -. k)));
       flush stdout;
       fn (j-1)
  in fn n

let eqn q drift a b =
  ((q *. a) +. (1.0 -. q) *. b) /. drift

let apply_column v acc q drift =
  List.fold_left (fun a b ->
    eqn q drift (List.hd a) b :: a
  ) [v] acc

let process_rows rowstart rowto q drift =
  output_value stdout rowto;
  let rec fn num acc =
    match num with
    |(-1) -> ()
    |i ->
      let v = input_value stdin in
      let c = apply_column v acc q drift in
      let acc = match c with
      |hd::tl when rowstart-i >= (rowstart - rowto) ->
        output_value stdout hd;
        flush stdout;
        tl
      |c -> c in
      fn (i-1) (List.rev acc)
  in
  fn rowstart []

let _ = 
  let float_arg x = float_of_string (Sys.argv.(x)) in
  let int_arg x = int_of_string (Sys.argv.(x)) in
  let [s;k;t;v;rf;cp] = List.map float_arg [1;2;3;4;5;6] in
  let [n;chunk;start] = List.map int_arg [7;8;9] in
  let q,u,d,drift = init_vals t v rf n in
  (match start with 
  |1 -> 
      let stkfn = c_stkval (float n) s u d in
      output_value stdout n;
      gen_initial_optvals stkfn n k cp
  |_ ->
      let rowstart = input_value stdin in
      if rowstart = 0 then
        printf "%.9f\n%!" (input_value stdin)
      else 
        let rowto = max 0 (rowstart - chunk) in
        process_rows rowstart rowto q drift
  ); flush stdout; flush stderr
