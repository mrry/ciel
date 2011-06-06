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

(* loop some spawns and derefs *)
let main a1 =
  let a1 = match a1 with [x] -> int_of_string x |_ -> assert false in
  let x = ref 0 in
  for i = 0 to 3 do
    let a1 = a1 * 10 in
    let a2_r = spawn (fun a2 -> a2 + 5) a1 in
    let a3_r = spawn (fun a2 -> a2 + 1) a1 in
    let res = (deref a2_r) + (deref a3_r) in
    x := !x + res
  done;
  !x

(* try a deref inside a spawn *)
let main2 a1 =
  let a1 = match a1 with [x] -> int_of_string x |_ -> assert false in
  let r = spawn (fun a1 -> a1 + 5) a1 in
  let a2 = spawn (fun a2 -> deref r + 5) a1 in
  deref a2

(* recursive *)
let main3 a1 =
  let a1 = match a1 with [x] -> int_of_string x |_ -> assert false in
  let rec loop acc = function
  |0 -> acc
  |n -> loop (deref (spawn ((+) 5) acc)) (n-1) in
  loop a1 10

(* opaque ref *)
let main4 a1 =
  let x_ref = spawn_ref (fun () ->
    Cwt.output (fun oc -> output_string oc "foobar123\n%!")) in
  input_ref input_line x_ref

(* streaming *)
let main5 a1 =
  let x_ref = spawn_ref (fun () ->
     Cwt.output ~stream:true (fun oc ->
       for i = 0 to 5 do
         Unix.sleep 1;
         fprintf oc "foobar %d\n%!" i;
       done
     )
   ) in
  let y_ref = spawn_ref (fun () ->
    input_ref (fun ic ->
      output ~stream:true (fun oc ->
        for i = 0 to 5 do
          let line = input_line ic in
          fprintf oc "LINE=%s\n%!" line
        done
      )
    ) x_ref
   ) in
  input_ref (fun ic ->
    let _ = input_line ic in
    input_line ic
  ) y_ref

let _ = Cwt.run (fun s -> s) main5
