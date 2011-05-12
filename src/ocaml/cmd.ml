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

let t =
  let read () = fail (Error "") in
  let write _ = fail (Error "") in
  ref { read; write }

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
let either cmds =
  match_lwt !t.read () with
  |`List [ `String meth; `Assoc args ] ->
     (try (List.assoc meth cmds) args 
     with Not_found -> fail (Failure ("unexpected command " ^ meth)))
  |_ -> fail (Failure "invalid command")

(* Expect a single command *)
let expect cmd fn = either [ (cmd, fn) ]

(* Send command to worker *)
let send meth args = !t.write (`List [ `String meth; `Assoc args ])

(* Send command to master and unpack response *)
let send_recv meth args fn =
  send meth args >>
  expect meth fn

(** Concrete Handlers *)

let exit ?(keep_process=false) () =
  send "exit" [ "keep_process", `String (if keep_process then "yes" else "no") ]

let allocate_output ?(prefix="obj") () =
  send_recv "allocate_output" [ "prefix", `String prefix ] (arg_int "index")

let open_output ~index ?(stream=false) ?(pipe=false) ?(sweetheart=false) () =
  send_recv "open_output" 
    [ "index", `Int index; "may_stream", `Bool stream;
      "may_pipe", `Bool pipe; "make_local_sweetheart", `Bool sweetheart ]
    (arg_string "filename")

let close_output ?size ~index () =
  send_recv "close_output" (("index", `Int index) ::
    (match size with |None -> [] |Some sz -> [("size", `Int sz)])) (arg_ref "ref")

let open_ref ~cref ?(sweetheart=false) () =
  send "open_ref" [ "ref", Cref.to_json cref; "make_sweetheart", `Bool sweetheart ] >>
  try_lwt 
    lwt filename = expect "open_ref" (arg_string "filename") in
    return (Some filename)
  with Error _ -> return None

let with_output ~index ?stream ?pipe ?sweetheart fn =
  lwt ofile = open_output ~index ?stream ?pipe ?sweetheart () in
  try_lwt
    Lwt_io.(with_file ofile ~mode:output fn) >>
    close_output ~index () 
  with exn -> begin
    close_output ~index () >>
    fail exn
  end

let with_new_output ?stream ?pipe ?sweetheart fn =
  lwt index = allocate_output () in
  with_output ~index ?stream ?pipe ?sweetheart fn

let output_value ~index v = 
  with_output ~index (fun oc ->
    Lwt_io.write_value oc ~flags:[Marshal.Closures] v)

let output_new_value v =
  with_new_output (fun oc ->
    Lwt_io.write_value oc ~flags:[Marshal.Closures] v)

let spawn ?(deps=[]) ~args ~n_outputs fn_ref =
  let deps = `List (List.map Cref.to_json deps) in
  send "spawn"  [ "executor_name", `String "ocaml"; "binary", `String "x"; 
    "n_outputs", `Int n_outputs; "args", `List args;
    "fn_ref", (Cref.to_json fn_ref); "extra_dependencies", deps ]  >>
  match_lwt !t.read () with 
  |`List [`String "spawn"; `List jl] -> return (List.map Cref.of_json jl)
  |_ -> return []

let tail_spawn ?(deps=[]) ~args ~n_outputs fn_ref =
  let deps = `List (List.map Cref.to_json deps) in
  send "tail_spawn" [ "executor_name", `String "ocaml"; "binary", `String "x";
    "n_outputs", `Int n_outputs; "fn_ref", (Cref.to_json fn_ref);
    "args", `List args; "extra_dependencies", deps ]

(* Input loop *)
let input () =
  try_lwt 
    either [
      "die", (fun args ->
        lwt reason = arg_string "reason" args in
        fail (Shutdown reason));
      "start_task", (fun args -> 
        let index = 0 in
        lwt rref = with_output ~index (fun oc -> Lwt_io.write_line oc "foobar") in
        printf "RREF[%d] = %s\n%!" index (Cref.to_string rref);
        lwt refs = spawn ~args:[`String "blah1"] ~n_outputs:1 rref in
        List.iter (fun r -> printf "Spawn ref = %s\n%!" (Cref.to_string r)) refs;
        fail (Shutdown "start_task"));
    ]
  with
  |Shutdown reason ->
    printf "Shutdown signalled\n%!";
    exit ()
  |exn ->
    printf "Internal error: %s\n%!" (Printexc.to_string exn);
    exit () >> fail exn

(* Read a JSON command over the executor FIFO *)
let read_framed_json ic =
  lwt len = Lwt_io.BE.read_int ic in
  let buf = String.create len in
  Lwt_io.read_into_exactly ic buf 0 len >>
  (return (printf "read[%d]: %s\n%!" len buf)) >>
  return (Basic.from_string buf :> json)

(* Write a JSON command over the executor FIFO *) 
let write_framed_json oc (json:json) () =
  let buf = to_string json in
  let len = String.length buf in
  printf "write[%d]: %s\n%!" len buf;
  Lwt_io.BE.write_int32 oc (Int32.of_int len) >>
  Lwt_io.write_from_exactly oc buf 0 len 

(* Connect to the executor FIFO, register a start
   function, and begin the command thread *)
let init fn =
  let parse_args () =
    match Sys.argv with
    | [| _; "--write-fifo"; wf; "--read-fifo"; rf |]
    | [| _; "--read-fifo"; rf; "--write-fifo"; wf |] ->
      (rf, wf)
    | _ ->
      failwith (sprintf "Unable to parse cmdline args: %s"
        (String.concat " " (Array.to_list Sys.argv))) in
  Sys.set_signal Sys.sigpipe Sys.Signal_ignore;
  let rf, wf = parse_args () in
  lwt wfd = Lwt_unix.openfile wf [Unix.O_WRONLY] 0 in
  lwt rfd = Lwt_unix.openfile rf [Unix.O_RDONLY] 0 in
  let ic = Lwt_io.(of_fd ~buffer_size:4000 ~mode:input rfd) in
  let oc = Lwt_io.(of_fd ~buffer_size:4000 ~mode:output wfd) in
  let ocm = Lwt_mutex.create () in
  let write json = Lwt_mutex.with_lock ocm (write_framed_json oc json) in
  let read () = read_framed_json ic in
  t := { read; write };
  input ()

