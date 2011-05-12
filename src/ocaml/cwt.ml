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

open Printf
open Lwt

(* Type of a reference *)
type 'a fut =Cref.t

exception Reference_not_ready
exception Spawn_failure

(* Obtain a concrete OCaml value from a reference *)
let repr cref =
  match_lwt Cmd.open_ref ~cref () with
  |Some filename -> Lwt_io.(with_file ~mode:input filename read_value)
  |None -> fail Reference_not_ready
 
(* Wait for all the references to become available and
   then run the function f *)
let bind (t:'a fut) (fn:'a -> 'b fut Lwt.t) : 'b fut Lwt.t =
  try_lwt
    lwt v = repr t in
    let res = Lwt_main.run (fn v) in
    return res
  with Reference_not_ready -> begin
    (* Tail spawn and exit immediately *)
    lwt oref = Cmd.output_new_value fn in
    Cmd.tail_spawn ~deps:[t] ~args:[] ~n_outputs:1 oref >>
    Cmd.exit () >>
    exit 1
  end

let spawn1 (t:'a) (fn:'a -> 'b fut Lwt.t) : 'b fut Lwt.t =
  let arg = Base64.encode (Marshal.to_string t []) in
  lwt oref = Cmd.output_new_value fn in
  lwt rrefs = Cmd.spawn ~args:[`String arg] ~n_outputs:1 oref in
  match rrefs with
  |[x] -> return x
  |_ -> fail Spawn_failure

let return1 (t:'a) : 'a fut Lwt.t =
  Cmd.output_value ~index:0 t

let return2 (t1:'a) (t2:'b) : ('a fut * 'b fut) Lwt.t =
  let r1 = Cmd.output_value ~index:0 t1 in
  let r2 = Cmd.output_value ~index:0 t2 in
  lwt r1 = r1 in lwt r2 = r2 in
  return (r1, r2)
  
  
