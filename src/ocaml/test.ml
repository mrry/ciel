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

open Cwt
open Printf

let main a1 =
  let a1 = a1 * 10 in
  let a2_r = spawn a1
    (fun a2 ->
       let a2 = a2 + 5 in
       return a2
    ) in
  let a3_r = spawn a1
    (fun a2 ->
       let a2 = a2 + 1 in
       return a2
    ) in
  bind a2_r (fun a2 ->
    bind a3_r (fun a3 ->
      let res = a2 + a3 in
      Printf.printf "res=%d\n%!" res;
      return res
    );
  )

let _ = Cwt.run main int_of_string string_of_int
