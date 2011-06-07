(*
 * Copyright (c) 2011 Anil Madhavapeddy <anil@recoil.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Cwt
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

let gen_initial_optvals n s u d k cp oc =
  for j = n downto 0 do
    let stkval = c_stkval (float n) s u d (float j) in
    output_value oc (max 0. (cp *. (stkval -. k)))
  done

let eqn q drift a b =
  ((q *. a) +. (1.0 -. q) *. b) /. drift

let apply_column v acc pos chunk q drift oc = 
  let v' = ref acc.(0) in
  acc.(0) <- v;
  let maxcol = min chunk pos in
  for idx = 1 to maxcol do
    let nv' = eqn q drift acc.(idx-1) !v' in
    v' := acc.(idx);
    acc.(idx) <- nv';
  done;
  if maxcol = chunk then output_value oc acc.(maxcol)

let process_rows rowstart rowto q drift ic oc =
  let chunk = rowstart - rowto in
  let acc = Array.create (chunk+1) 0.0 in
  for pos = 0 to rowstart do
    let v = input_value ic in
    apply_column v acc pos chunk q drift oc;
  done

let main args = 
  let s = 100. in
  let k = 100. in
  let t = 1. in
  let v = 0.3 in
  let rf = 0.03 in
  let cp = -1. in
  let (n,chunk) = match args with
    |[n;c] -> (int_of_string n, int_of_string c)
    |_ -> failwith "args" in
  let q,u,d,drift = init_vals t v rf n in
  let first = Cwt.spawn_ref (fun () ->
    Cwt.output ~stream:true (gen_initial_optvals n s u d k cp)
  ) in
  let rec loop oref = function
  |0 ->
     input_ref (fun ic -> sprintf "%.9f" (input_value ic)) oref
  |n ->
     let rowto = max 0 (n - chunk) in
     let oref2 = Cwt.spawn_ref (fun () ->
       input_ref (fun ic ->
         Cwt.output ~stream:true (process_rows n rowto q drift ic)
       ) oref) in
     loop oref2 (n-chunk)
  in
  loop first n

let _ = Cwt.run (fun s -> s) main
