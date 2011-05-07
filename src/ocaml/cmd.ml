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

(* Retrieve JSON argument from worker response *)
let arg_get err fn key args =
  try return (fn (List.assoc key args))
  with Not_found -> fail (Error err)

(* Extract an argument string value *)
let arg_string = arg_get "string" (function `String x -> x |_ -> raise Not_found)

(* Extract an argument integer value *)
let arg_int = arg_get "int" (function `Int x -> x |_ -> raise Not_found)

(* Extract a argument reference *)
let arg_ref k v = arg_get "ref" (fun x -> x) k v >|= Cref.of_json

(* Dispatch an incoming command to a set of expected ones *)
let either t cmds =
  match_lwt t.read () with
  |`List [ `String meth; `Assoc args ] ->
     (try (List.assoc meth cmds) args 
     with Not_found -> fail (Failure ("unexpected command " ^ meth)))
  |_ -> fail (Failure "invalid command")

(* Expect a single command *)
let expect t cmd fn = either t [ (cmd, fn) ]

(* Send command to worker *)
let send t meth args = t.write (`List [ `String meth; `Assoc args ])

(* Send command to master and unpack response *)
let send_recv t meth args fn =
  send t meth args >>
  expect t meth fn

(** Concrete Handlers *)

let exit ?(keep_process=false) t =
  send t "exit" [ "keep_process", `String (if keep_process then "yes" else "no") ]

let open_output ~index ?(stream=false) ?(pipe=false) ?(sweetheart=false) t =
  send_recv t "open_output" 
    [ "index", `Int index; "may_stream", `Bool stream;
      "may_pipe", `Bool pipe; "make_local_sweetheart", `Bool sweetheart ]
    (arg_string "filename")

let allocate_output ?(prefix="obj") t =
  send_recv t "allocate_output" [ "prefix", `String prefix ] (arg_int "index")

let close_output ?size ~index t =
  send_recv t "close_output" (("index", `Int index) ::
    (match size with |None -> [] |Some sz -> [("size", `Int sz)])) (arg_ref "ref")

let open_ref ~cref ?(sweetheart=false) t =
  send t "open_ref" [ "ref", Cref.to_json cref; "make_sweetheart", `Bool sweetheart ] >>
  try_lwt 
    lwt filename = expect t "open_ref" (arg_string "filename") in
    return (Some filename)
  with Error _ -> return None

let with_output ~index ?stream ?pipe ?sweetheart t fn =
  lwt ofile = open_output ~index ?stream ?pipe ?sweetheart t in
  try_lwt
    Lwt_io.(with_file ofile ~mode:output fn) >>
    close_output ~index t
  with exn -> begin
    close_output ~index t >>
    fail exn
  end
  
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
        let index = 0 in
        lwt rref = with_output ~index t (fun oc -> Lwt_io.write_line oc "foobar") in
        printf "RREF[%d] = %s\n%!" index (match rref with |None -> "??" |Some x -> Cref.to_string x);
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

 let read_framed_json ic =
  lwt len = Lwt_io.BE.read_int ic in
  let buf = String.create len in
  Lwt_io.read_into_exactly ic buf 0 len >>
  (return (printf "read[%d]: %s\n%!" len buf)) >>
  return (Basic.from_string buf :> json)
 
let write_framed_json oc (json:json) () =
  let buf = to_string json in
  let len = String.length buf in
  printf "write[%d]: %s\n%!" len buf;
  Lwt_io.BE.write_int32 oc (Int32.of_int len) >>
  Lwt_io.write_from_exactly oc buf 0 len 

let fifo_t wfd rfd =
  let ic = Lwt_io.(of_fd ~buffer_size:4000 ~mode:input rfd) in
  let oc = Lwt_io.(of_fd ~buffer_size:4000 ~mode:output wfd) in
  let ocm = Lwt_mutex.create () in
  let write json = Lwt_mutex.with_lock ocm (write_framed_json oc json) in
  let read () = read_framed_json ic in
  input write read

