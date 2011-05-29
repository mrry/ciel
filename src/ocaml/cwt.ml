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

(* A reference with an associated marshal-able OCaml type *)
type 'a ref = Cref.t
(* A reference that is opaque and only accessible via I/O channels *)
type opaque_ref = Cref.t

let p = Delimcc.new_prompt ()

(* Do something with a reference, and if it is not 
   available, tailspawn a new call *)
let with_ref fn t =
  try fn t
  with Cmd.Reference_not_available ->
    Delimcc.shift0 p (fun k ->
      let oref = Cmd.with_new_output
        (fun oc -> Delimcc.output_delim_value oc k) in
      Cmd.tail_spawn ~deps:[t] ~args:[] ~n_outputs:1 oref;
      Cmd.exit ()
    );
    fn t

(* Dereference and unmarshal a reference *)
let deref (t:'a ref) : 'a =
  with_ref (fun cref -> Cmd.input_value ~cref) t

(* Force the evaluation of an opaque reference *)
let force (t:opaque_ref) =
  with_ref (fun cref -> ()) t

(* Read from an opaque reference *)
let input_ref fn (cref:opaque_ref) =
  with_ref (fun cref -> 
    match Cmd.open_ref ~cref () with
    |Some fname -> Cmd.with_input_file fname fn
    |None -> raise Cmd.Reference_not_available
  ) cref

let spawn (fn:'a->'b) (t:'a): 'b ref =
  let fn' t = Cmd.output_value ~index:0 (fn t) in
  let arg = Base64.encode (Marshal.to_string t []) in
  let oref = Cmd.output_new_value fn' in
  let rrefs = Cmd.spawn ~args:[`String arg] ~n_outputs:1 oref in
  match rrefs with |[x] -> x |_ -> raise (Failure "spawn")

let spawn_ref ?(stream=false) ?(pipe=false) (fn:opaque_ref->unit) (t:opaque_ref) : opaque_ref =
  let fn' t = Cmd.output_value ~index:0 (fn t) in
  let oref = Cmd.output_new_value fn' in
  let rrefs = Cmd.spawn ~args:[] ~deps:[t] ~n_outputs:1 oref in
  match rrefs with |[x] -> x |_ -> raise (Failure "spawn_ref")

let spawn_ref0 ?(stream=false) ?(pipe=false) (fn:out_channel->unit) : opaque_ref =
  let fn' () = Cmd.with_output ~index:0 ~stream ~pipe fn in
  let oref = Cmd.output_new_value fn' in
  let rrefs = Cmd.spawn ~args:[] ~deps:[] ~n_outputs:1 oref in
  match rrefs with |[x] -> x |_ -> raise (Failure "spawn_ref")

let run ofn fn =
  let callback args =
    match List.mem_assoc "fn_ref" args with
    |false ->
       (* main start *)
       let argv = match List.assoc "args" args with
         |`List l ->
            List.map (function
               |`String x -> x
               |`Int x -> string_of_int x
               |x -> Yojson.to_string x) l
         |_ -> [] in
       Delimcc.push_prompt p (fun () ->
         let result = ofn (fn argv) in
         ignore(Cmd.with_output ~index:0 (fun oc -> output_string oc result))
       )
    |true -> begin
       match List.assoc "args" args with
       |`List [`String arg1 ] ->
         (* non-streaming spawn *)
         let cref = Cref.of_json (List.assoc "fn_ref" args) in
         let fn : ('a -> unit) = Cmd.input_value ~cref in
         let arg1 : 'a = Marshal.from_string (Base64.decode arg1) 0 in
         ignore(Delimcc.push_prompt p (fun () -> fn arg1))
       |`List [] ->
         (* tail-spawn continuation *)
         let cref = Cref.of_json (List.assoc "fn_ref" args) in
         let fn : ('a -> unit) = Cmd.input_value ~cref in
         ignore(Delimcc.push_prompt p fn)
       |_ -> assert false
    end
  in ignore(Cmd.init callback)
