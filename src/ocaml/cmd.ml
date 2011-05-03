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
open Yojson

exception Shutdown of string
exception Error of string

type t = {
  read: unit -> json Lwt.t;
  write: json -> unit Lwt.t
}

type output_ref = string

module Out = struct
  (* General form of a JSON command to the master *)
  let cmd meth args =
    `List [ `String meth; `Assoc args ]

  (* Exit command to signal quit *)
  (* XXX why is keep_process not a JSON bool? *)
  let exit ?(keep_process=false) () =
    cmd "exit" [ "keep_process", `String (if keep_process then "yes" else "no") ]

  (* Open output reference *)
  let open_output ~index ~stream ~pipe ~sweetheart =
    cmd "open_output" 
     [ "index", `Int index; "may_stream", `Bool stream;
       "may_pipe", `Bool pipe;
       "make_local_sweetheart", `Bool sweetheart ]

  (* Allocate new output object *)
  let allocate_output ~prefix =
   cmd "allocate_output" [ "prefix", `String prefix ]

end

(** Combinators to help process commands *)

let get_arg err fn key args =
  try return (fn (List.assoc key args))
  with Not_found -> fail (Error err)

(* Extract an argument string value *)
let arg_string =
  get_arg "arg_string" (function `String x -> x |_ -> raise Not_found)

(* Extract an argument integer value *)
let arg_int =
  get_arg "arg_int" (function `Int x -> x |_ -> raise Not_found)

(* Extract a argument reference value *)
let arg_output_ref k v : output_ref Lwt.t = arg_string k v
   
(* Dispatch an incoming command to a set of expected ones *)
let either t cmds =
  match_lwt t.read () with
  |`List [ `String meth; `Assoc args ] ->
     (try (List.assoc meth cmds) args 
     with Not_found -> fail (Failure ("unexpected command " ^ meth)))
  |_ -> fail (Failure "invalid command")

(* Expect a single command *)
let expect t cmd fn =
  either t [ (cmd, fn) ]

(** Concrete Handlers *)

let exit t =
  t.write (Out.exit ())

let get_output_ref ~index ?(stream=false) ?(pipe=false) ?(sweetheart=false) t  =
  t.write (Out.open_output ~index ~stream ~pipe ~sweetheart) >>
  expect t "open_output" (arg_output_ref "filename")

let allocate_output ?(prefix="obj") t =
  t.write (Out.allocate_output ~prefix) >>
  expect t "allocate_output" (arg_int "index")

(* Input loop *)
let input write read =
  let t = { write; read } in
  try_lwt 
    either t [
      "die", (fun args ->
        lwt reason = arg_string "reason" args in
        fail (Shutdown reason)
      );
      "start_task", (fun args -> 
        lwt index = allocate_output t in
        lwt oref = get_output_ref ~index t in
        printf "OREF = %s\n%!" oref;
        fail (Shutdown "start_task")
      );
    ]
  with
  |Shutdown reason ->
    printf "Shutdown signalled\n%!";
    exit t
  |Error reason as exn ->
    printf "Error: %s\n%!" reason;
    exit t >> fail exn
  |exn ->
    printf "Internal error: %s\n%!" (Printexc.to_string exn);
    exit t >> fail exn
 
