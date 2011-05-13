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
type 'a ref = Cref.t
type ('a,'b) fut = 
  |Ref of 'b ref
  |Future of ('a,'b) fut * ('a -> ('a,'b) fut Lwt.t)

exception Spawn_failure

(* Obtain a concrete OCaml value from a reference *)
let repr = function
  |Future _ -> return None
  |Ref cref -> begin
    match_lwt Cmd.open_ref ~cref () with
    |Some filename -> Lwt_io.(with_file ~mode:input filename read_value)
    |None -> return None
  end
 
(* Wait for all the references to become available and
   then run the function f *)
let bind (t:('a,'b) fut) fn =
  match_lwt repr t with
  |Some v -> return (Lwt_main.run (fn v))
  |None -> return (Future (t, fn))

let spawn1 (t:'a) (fn:'a -> ('a,'b) fut Lwt.t) : ('a,'b) fut Lwt.t =
  let arg = Base64.encode (Marshal.to_string t []) in
  lwt oref = Cmd.output_new_value fn in
  lwt rrefs = Cmd.spawn ~args:[`String arg] ~n_outputs:1 oref in
  match rrefs with
  |[x] -> return (Ref x)
  |_ -> fail Spawn_failure

let return1 (t:'a) : ('a,'b) fut Lwt.t =
  lwt x = Cmd.output_value ~index:0 t in
  return (Ref x)

let return2 (t1:'a) (t2:'b) : ('a ref * 'b ref) Lwt.t =
  let r1 = Cmd.output_value ~index:0 t1 in
  let r2 = Cmd.output_value ~index:0 t2 in
  lwt r1 = r1 in lwt r2 = r2 in
  return (r1, r2)
  
