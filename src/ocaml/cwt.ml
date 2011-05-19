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

(* Type of a reference *)
type 'a ref = Cref.t

exception Spawn_failure

let repr (t:'a ref) : 'a =
  Cmd.input_value ~cref:t
     
(* Wait for all the references to become available and
   then run the function f *)
let bind t fn : 'a ref =
  try
    fn (repr t)
  with Cmd.Reference_not_available -> begin
    (* tail spawn *)
    let fn' () = fn (repr t) in
    let oref = Cmd.output_new_value fn' in
    Cmd.tail_spawn ~deps:[t] ~args:[] ~n_outputs:1 oref;
    Cmd.exit ()
  end

let spawn (t:'a) (fn:'a -> 'b ref) : 'b ref =
  let arg = Base64.encode (Marshal.to_string t []) in
  let oref = Cmd.output_new_value fn in
  let rrefs = Cmd.spawn ~args:[`String arg] ~n_outputs:1 oref in
  match rrefs with
  |[x] -> x
  |_ -> raise Spawn_failure

let return (t:'a) : 'a ref =
  Cmd.output_value ~index:0 t

let run fn ifn ofn =
  let main arg1 =
    let _ = bind (spawn (ifn arg1) fn) (fun res ->
      Cmd.with_output ~index:0 (fun oc -> output_string oc (ofn res))
    ) in ()
  in Cmd.init main
