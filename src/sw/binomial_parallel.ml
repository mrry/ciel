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

let calc_stkval n s u d =
  let stkval = create float64 c_layout (n+1) (n+1) in
  set stkval 0 0 s;
  for i = 1 to n do
    set stkval i 0 (get stkval (i-1) 0 *. u);
    for j = 1 to i do
      set stkval i j (get stkval (i-1) (j-1) *. d)
    done
  done;
  stkval

let gen_initial_optvals stkval n k cp =
  Array.init (n+1) (fun j -> max 0. (cp *. (get stkval n j -. k)))

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
  match start with 
  |1 -> 
      let stkval = calc_stkval n s u d in
      let x = List.rev (Array.to_list (gen_initial_optvals stkval n k cp)) in
      output_value stdout n;
      List.iter (output_value stdout) x
  |_ ->
      let rowstart = input_value stdin in
      if rowstart = 0 then
        printf "%14f\n%!" (input_value stdin)
      else 
        let rowto = max 0 (rowstart - chunk) in
        process_rows rowstart rowto q drift
