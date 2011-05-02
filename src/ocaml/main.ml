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

let read_fifo_t rfd =
  let ic = Lwt_io.of_fd ~buffer_size:12000 ~mode:Lwt_io.input rfd in
  let rec loop () =
    try_lwt
      lwt len = Lwt_io.BE.read_int32 ic in
      let len = Int32.to_int len in
      let buf = String.create len in
      lwt () = Lwt_io.read_into_exactly ic buf 0 len in
      printf "read raw: %s\n%!" buf;
      let json = (Yojson.Basic.from_string buf :> Yojson.json) in
      Yojson.pretty_to_channel ~std:true stdout json;
      printf "read json\n%!";
      loop ()
    with exn ->  begin
      printf "EXN %s\n%!" (Printexc.to_string exn);
      loop ()
    end
  in loop () 

let parse_args () =
  match Sys.argv with
    | [| _; "--write-fifo"; wf; "--read-fifo"; rf |]
    | [| _; "--read-fifo"; rf; "--write-fifo"; wf |] ->
      (rf, wf)
    | _ ->
      failwith (Printf.sprintf "Unable to parse cmdline args: %s"
        (String.concat " " (Array.to_list Sys.argv)))

let main () =
  Sys.set_signal Sys.sigpipe Sys.Signal_ignore;
  eprintf "START ocamltest\n%!";
  let rf, wf = parse_args () in
  eprintf "ARGS parsed ocamltest\n%!";
  lwt wfd = Lwt_unix.openfile wf [Unix.O_WRONLY] 0 in
  lwt rfd = Lwt_unix.openfile rf [Unix.O_RDONLY] 0 in
  let read_t = read_fifo_t rfd in
  return read_t
 
let _ = 
  Lwt_main.run (main ())
