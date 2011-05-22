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
  let r = spawn (fun a1 -> a1 + 5) a1 in
  let a2 = spawn (fun a2 -> deref r + 5) a1 in
  deref a2

(* recursive *)
let main3 a1 =
  let rec loop acc = function
  |0 -> acc
  |n -> loop (deref (spawn ((+) 5) acc)) (n-1) in
  loop a1 10

let _ = Cwt.run int_of_string string_of_int main3
