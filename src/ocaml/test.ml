open Cwt

let main a1 : int fut Lwt.t =
  let a1 = a1 * 10 in
  lwt a2_r = spawn1 a1
    (fun a2 ->
       let a2 = a2 + 5 in
       return1 a2
    ) in
  lwt a3_r = spawn1 a1
    (fun a2 ->
       let a2 = a2 + 1 in
       return1 a2
    ) in
  bind a2_r (fun a2 ->
    bind a3_r (fun a3 ->
      let res = a2 + a3 in
      Printf.printf "res=%d\n%!" res;
      return1 res
    );
  )

