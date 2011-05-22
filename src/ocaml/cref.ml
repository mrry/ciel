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

open Yojson

type id = string

type t =
  |Concrete of json
  |Future of string
  |Stream
  |Sweetheart
  |Value of id * string 
  |Completed
  |Null

let of_tuple = function
  |`List [ `String "val"; `String k; `String v ] -> Value (k, (Base64.decode v))
  |`List (`String "c2" :: tl) as x -> Concrete x
  |`List [ `String "f2"; `String id ] -> Future id
  |_ -> Null

let of_json = function
  |`Assoc json -> (try of_tuple (List.assoc "__ref__" json) with Not_found -> Null)
  |_ -> Null

let to_json t =
  let j = match t with
    |Value (k,v) -> `List [`String "val"; `String k; `String (Base64.encode v)]
    |Concrete j -> j
    |Future id -> `List [`String "f2"; `String id]
    |Null -> `Null
    |_ -> failwith "Cannot handle this ref type yet" in
  `Assoc [ "__ref__", j ]

let to_string t = Yojson.to_string (to_json t)
