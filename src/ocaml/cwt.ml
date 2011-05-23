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

type 'a ref = Cref.t

let p = Delimcc.new_prompt ()

let rec deref (t:'a ref) : 'a =
  try Cmd.input_value ~cref:t 
  with Cmd.Reference_not_available ->
    Delimcc.shift0 p (fun k ->
      let oref = Cmd.with_new_output
        (fun oc -> Delimcc.output_delim_value oc k) in
      Cmd.tail_spawn ~deps:[t] ~args:[] ~n_outputs:1 oref;
      Cmd.exit ()
    );
    deref t

let spawn ?(stream=false) (fn:'a -> 'b) (t:'a) : 'b ref =
  let arg = Base64.encode (Marshal.to_string t []) in
  let oref = Cmd.output_new_value fn in
  let rrefs = Cmd.spawn ~args:[`Bool stream; `String arg] ~n_outputs:1 oref in
  match rrefs with |[x] -> x |_ -> raise (Failure "spawn")

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
      |`List [`Bool false; `String arg1 ] ->
        (* non-streaming spawn *)
        let cref = Cref.of_json (List.assoc "fn_ref" args) in
        let fn : ('a -> unit) = Cmd.input_value ~cref in
        let arg1 : 'a = Marshal.from_string (Base64.decode arg1) 0 in
        let result = Delimcc.push_prompt p (fun () -> fn arg1) in
        ignore(Cmd.output_value ~index:0 result)
      |`List [] ->
        (* tail-spawn continuation *)
        let cref = Cref.of_json (List.assoc "fn_ref" args) in
        let fn : ('a -> unit) = Cmd.input_value ~cref in
        let result = Delimcc.push_prompt p fn in
        ignore(Cmd.output_value ~index:0 result)
      |_ -> assert false
    end
  in ignore(Cmd.init callback)
