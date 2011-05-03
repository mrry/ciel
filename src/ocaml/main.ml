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
  Cmd.input write read

let parse_args () =
  match Sys.argv with
    | [| _; "--write-fifo"; wf; "--read-fifo"; rf |]
    | [| _; "--read-fifo"; rf; "--write-fifo"; wf |] ->
      (rf, wf)
    | _ ->
      failwith (sprintf "Unable to parse cmdline args: %s"
        (String.concat " " (Array.to_list Sys.argv)))

let main () =
  Sys.set_signal Sys.sigpipe Sys.Signal_ignore;
  let rf, wf = parse_args () in
  lwt wfd = Lwt_unix.openfile wf [Unix.O_WRONLY] 0 in
  lwt rfd = Lwt_unix.openfile rf [Unix.O_RDONLY] 0 in
  fifo_t wfd rfd
 
let _ = 
  Lwt_main.run (main ())
