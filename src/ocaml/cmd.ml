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
open Yojson

exception Shutdown of string
exception Error of string
exception Reference_not_available

type t = {
  read: unit -> json;
  write: json -> unit;
}

let t =
  let read () = raise (Error "") in
  let write _ = raise (Error "") in
  ref { read; write }

(* Retrieve JSON argument from worker response *)
let arg_get err fn key args =
  try fn (List.assoc key args)
  with Not_found -> raise (Error err)

(* Extract an argument string value *)
let arg_string = arg_get "string" (function `String x -> x |_ -> raise Not_found)

(* Extract an argument integer value *)
let arg_int = arg_get "int" (function `Int x -> x |_ -> raise Not_found)

(* Extract a argument reference *)
let arg_ref k v = Cref.of_json (arg_get "ref" (fun x -> x) k v)

(* Dispatch an incoming command to a set of expected ones *)
let either cmds =
  match !t.read () with
  |`List [ `String meth; `Assoc args ] ->
     (try (List.assoc meth cmds) args 
     with Not_found -> raise (Failure ("unexpected command " ^ meth)))
  |_ -> raise (Failure "invalid command")

(* Expect a single command *)
let expect cmd fn = either [ (cmd, fn) ]

(* Send command to worker *)
let send meth args = !t.write (`List [ `String meth; `Assoc args ])

(* Send command to master and unpack response *)
let send_recv meth args fn =
  send meth args;
  expect meth fn

(** Concrete Handlers *)

let exit ?(keep_process=false) () =
  send "exit" [ "keep_process", `String (if keep_process then "yes" else "no") ];
  exit 0

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
  send "open_ref" [ "ref", Cref.to_json cref; "make_sweetheart", `Bool sweetheart ];
  try
    let filename = expect "open_ref" (arg_string "filename") in
    Some filename
  with Error _ ->  None

let with_output_file fname fn =
  let oc = open_out fname in
  try
    let res = fn oc in
    close_out oc;
    res
  with exn -> begin
    close_out oc;
    raise exn
  end

let with_input_file fname fn =
  let oc = open_in fname in
  try
    let res = fn oc in
    close_in oc;
    res
  with exn -> begin
    close_in oc;
    raise exn
  end

let with_output ~index ?stream ?pipe ?sweetheart fn =
  let ofile = open_output ~index ?stream ?pipe ?sweetheart () in
  try
    with_output_file ofile fn;
    close_output ~index () 
  with exn -> begin
    ignore (close_output ~index ());
    raise exn
  end

let with_new_output ?stream ?pipe ?sweetheart fn =
  let index = allocate_output () in
  with_output ~index ?stream ?pipe ?sweetheart fn

let output_value ~index v = 
  with_output ~index (fun oc -> Marshal.to_channel oc v [Marshal.Closures])

let output_new_value v =
  with_new_output (fun oc -> Marshal.to_channel oc v [Marshal.Closures])

let input_value ~cref =
  match open_ref ~cref () with
  |Some filename -> with_input_file filename Marshal.from_channel
  |None -> raise Reference_not_available

let spawn ?(deps=[]) ~args ~n_outputs fn_ref =
  let deps = `List (List.map Cref.to_json deps) in
  send "spawn" [ "executor_name", `String "ocaml"; "binary", `String "x"; 
    "n_outputs", `Int n_outputs; "args", `List args;
    "fn_ref", (Cref.to_json fn_ref); "extra_dependencies", deps ];
  match !t.read () with 
  |`List [`String "spawn"; `List jl] -> List.map Cref.of_json jl
  |_ -> []

let tail_spawn ?(deps=[]) ~args ~n_outputs fn_ref =
  let deps = `List (List.map Cref.to_json deps) in
  send "tail_spawn" [ "executor_name", `String "ocaml"; "binary", `String "x";
    "n_outputs", `Int n_outputs; "fn_ref", (Cref.to_json fn_ref);
    "args", `List args; "extra_dependencies", deps ]

(* Input loop *)
let input main =
  try
    either [
      "die", (fun args -> raise (Shutdown (arg_string "reason" args)));
      "start_task", (fun args -> 
        (* Check if this is a new job or a continuation *)
        let arg1 = match List.assoc "args" args with
          |`List [`String arg1] -> Some arg1
          |`List [`Int arg1] -> Some (string_of_int arg1)
          |_ -> None in
        let has_fnref = List.mem_assoc "fn_ref" args in
        match (has_fnref, arg1) with
        |false, Some arg1 -> (* fresh task *)
          printf "Start_task: main\n%!";
          main arg1
        |false, None ->
          printf "Start_task: main blank arg\n%!";
          main ""
        |true, Some arg1 -> (* continuation *)
          let fn_ref = Cref.of_json (List.assoc "fn_ref" args) in
          let fn : ('a -> unit) = input_value ~cref:fn_ref in
          let arg1 = Marshal.from_string (Base64.decode arg1) 0 in
          printf "Start_task: cont\n%!";
          fn arg1 
        |true, None -> (* tail spawn *)
          let fn_ref = Cref.of_json (List.assoc "fn_ref" args) in
          let fn : ('a -> unit) = input_value ~cref:fn_ref in
          printf "Start_task: tail\n%!";
          fn ()
        );
    ];
    exit ()
  with
  |Shutdown reason ->
    printf "Shutdown signalled: %s\n%!" reason;
    exit ()
  |exn ->
    printf "Internal error: %s\n%!" (Printexc.to_string exn);
    exit ()

(* Read a JSON command over the executor FIFO *)
let read_framed_json ic =
  let len = input_binary_int ic in
  let buf = String.create len in
  really_input ic buf 0 len;
  printf "read[%d]: %s\n%!" len buf;
  (Basic.from_string buf :> json)

(* Write a JSON command over the executor FIFO *) 
let write_framed_json oc (json:json) =
  let buf = to_string json in
  let len = String.length buf in
  printf "write[%d]: %s\n%!" len buf;
  output_binary_int oc len;
  output_string oc buf;
  flush oc

(* Connect to the executor FIFO, register a start
   function, and begin the command thread *)
let init main =
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
  let oc = open_out_bin wf in
  let ic = open_in_bin rf in
  let write json = write_framed_json oc json in
  let read () = read_framed_json ic in
  t := { read; write };
  input main

