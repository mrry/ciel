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

type ('a,'b) fut

exception Spawn_failure

val repr : ('a,'b) fut -> 'b option Lwt.t
val bind : ('a,'b) fut -> ('a -> ('a,'b) fut Lwt.t) -> ('a,'b) fut Lwt.t
val spawn1 : 'a -> ('a -> ('a,'b) fut Lwt.t) -> ('a,'b) fut Lwt.t
val return1 : 'a -> ('a,'b) fut Lwt.t
(* val run : 'a -> ('a -> string) -> ('a -> 'b) -> 'b *)
