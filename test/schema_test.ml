open! Core
open Polars

let%expect_test "check serializations" =
  (* TODO: use quickcheck to randomly generate and verify. *)
  let some_fields =
    [ Data_type.Boolean
    ; UInt8
    ; UInt16
    ; UInt32
    ; UInt64
    ; Int8
    ; Int16
    ; Int32
    ; Int64
    ; Float32
    ; Float64
    ; Utf8
    ; Binary
    ; Date
    ]
    @ List.map Data_type.Time_unit.all ~f:(fun time_unit ->
        Data_type.Datetime (time_unit, None))
    @ List.map Data_type.Time_unit.all ~f:(fun time_unit -> Data_type.Duration time_unit)
    @ [ Time
      ; List Boolean
      ; Null
      ; Categorical None
      ; Categorical (Some (Local [ Some "a"; None; Some "c" ]))
      ; Unknown
      ]
    |> List.map ~f:(fun data_type ->
         let name = [%sexp_of: Data_type.t] data_type |> Sexp.to_string in
         name, data_type)
  in
  Schema.create some_fields |> [%sexp_of: Schema.t] |> print_s;
  [%expect
    {|
    ((Boolean Boolean) (UInt8 UInt8) (UInt16 UInt16) (UInt32 UInt32)
     (UInt64 UInt64) (Int8 Int8) (Int16 Int16) (Int32 Int32) (Int64 Int64)
     (Float32 Float32) (Float64 Float64) (Utf8 Utf8) (Binary Binary) (Date Date)
     ("(Datetime Nanoseconds())" (Datetime Nanoseconds ()))
     ("(Datetime Microseconds())" (Datetime Microseconds ()))
     ("(Datetime Milliseconds())" (Datetime Milliseconds ()))
     ("(Duration Nanoseconds)" (Duration Nanoseconds))
     ("(Duration Microseconds)" (Duration Microseconds))
     ("(Duration Milliseconds)" (Duration Milliseconds)) (Time Time)
     ("(List Boolean)" (List Boolean)) (Null Null)
     ("(Categorical())" (Categorical ()))
     ("(Categorical((Local((a)()(c)))))" (Categorical ((Local ((a) () (c))))))
     (Unknown Unknown)) |}]
;;

let%expect_test _ =
  let schema =
    Schema.create
      [ "first_name", Categorical None
      ; "gender", Categorical None
      ; "type", Categorical None
      ; "state", Categorical None
      ; "party", Categorical None
      ; "birthday", Date
      ]
  in
  let dataset =
    Data_frame.read_csv_exn
      ~schema
      ~try_parse_dates:true
      "./data/legislators-historical.csv"
  in
  Data_frame.print dataset;
  [%expect
    {|
    shape: (12_136, 36)
    ┌────────────┬──────────┬───────────┬────────┬───┬────────────┬────────────┬──────────┬────────────┐
    │ last_name  ┆ first_na ┆ middle_na ┆ suffix ┆ … ┆ ballotpedi ┆ washington ┆ icpsr_id ┆ wikipedia_ │
    │ ---        ┆ me       ┆ me        ┆ ---    ┆   ┆ a_id       ┆ _post_id   ┆ ---      ┆ id         │
    │ str        ┆ ---      ┆ ---       ┆ str    ┆   ┆ ---        ┆ ---        ┆ i64      ┆ ---        │
    │            ┆ cat      ┆ str       ┆        ┆   ┆ str        ┆ str        ┆          ┆ str        │
    ╞════════════╪══════════╪═══════════╪════════╪═══╪════════════╪════════════╪══════════╪════════════╡
    │ Bassett    ┆ Richard  ┆ null      ┆ null   ┆ … ┆ null       ┆ null       ┆ 507      ┆ Richard    │
    │            ┆          ┆           ┆        ┆   ┆            ┆            ┆          ┆ Bassett    │
    │            ┆          ┆           ┆        ┆   ┆            ┆            ┆          ┆ (Delaware  │
    │            ┆          ┆           ┆        ┆   ┆            ┆            ┆          ┆ politi…    │
    │ Bland      ┆ Theodori ┆ null      ┆ null   ┆ … ┆ null       ┆ null       ┆ 786      ┆ Theodorick │
    │            ┆ ck       ┆           ┆        ┆   ┆            ┆            ┆          ┆ Bland (con │
    │            ┆          ┆           ┆        ┆   ┆            ┆            ┆          ┆ gressman)  │
    │ Burke      ┆ Aedanus  ┆ null      ┆ null   ┆ … ┆ null       ┆ null       ┆ 1260     ┆ Aedanus    │
    │            ┆          ┆           ┆        ┆   ┆            ┆            ┆          ┆ Burke      │
    │ Carroll    ┆ Daniel   ┆ null      ┆ null   ┆ … ┆ null       ┆ null       ┆ 1538     ┆ Daniel     │
    │            ┆          ┆           ┆        ┆   ┆            ┆            ┆          ┆ Carroll    │
    │ …          ┆ …        ┆ …         ┆ …      ┆ … ┆ …          ┆ …          ┆ …        ┆ …          │
    │ Flores     ┆ Mayra    ┆ null      ┆ null   ┆ … ┆ Mayra      ┆ null       ┆ null     ┆ Mayra      │
    │            ┆          ┆           ┆        ┆   ┆ Flores     ┆            ┆          ┆ Flores     │
    │ Sempolinsk ┆ Joseph   ┆ null      ┆ null   ┆ … ┆ Joe Sempol ┆ null       ┆ null     ┆ Joe Sempol │
    │ i          ┆          ┆           ┆        ┆   ┆ inski      ┆            ┆          ┆ inski      │
    │ Inhofe     ┆ James    ┆ M.        ┆ null   ┆ … ┆ Jim Inhofe ┆ null       ┆ 15424    ┆ Jim Inhofe │
    │ Sasse      ┆ Benjamin ┆ Eric      ┆ null   ┆ … ┆ Ben Sasse  ┆ null       ┆ 41503    ┆ Ben Sasse  │
    └────────────┴──────────┴───────────┴────────┴───┴────────────┴────────────┴──────────┴────────────┘ |}];
  Data_frame.schema dataset |> [%sexp_of: Schema.t] |> print_s;
  [%expect
    {|
    ((last_name Utf8)
     (first_name (Categorical ((Global <opaque> <opaque> <opaque>))))
     (middle_name Utf8) (suffix Utf8) (nickname Utf8) (full_name Utf8)
     (birthday Date) (gender (Categorical ((Global <opaque> <opaque> <opaque>))))
     (type (Categorical ((Global <opaque> <opaque> <opaque>))))
     (state (Categorical ((Global <opaque> <opaque> <opaque>)))) (district Int64)
     (senate_class Int64)
     (party (Categorical ((Global <opaque> <opaque> <opaque>)))) (url Utf8)
     (address Utf8) (phone Utf8) (contact_form Utf8) (rss_url Utf8)
     (twitter Utf8) (twitter_id Utf8) (facebook Utf8) (youtube Utf8)
     (youtube_id Utf8) (mastodon Utf8) (bioguide_id Utf8) (thomas_id Utf8)
     (opensecrets_id Utf8) (lis_id Utf8) (fec_ids Utf8) (cspan_id Utf8)
     (govtrack_id Int64) (votesmart_id Utf8) (ballotpedia_id Utf8)
     (washington_post_id Utf8) (icpsr_id Int64) (wikipedia_id Utf8)) |}]
;;
