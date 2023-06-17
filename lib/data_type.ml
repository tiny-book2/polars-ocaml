open! Core

module Time_unit = struct
  type t =
    | Nanoseconds
    | Microseconds
    | Milliseconds
  [@@deriving sexp, enumerate]
end

module Uuid = struct
  type t
end

module Rev_mapping = struct
  type t =
    | Global of
        ((int * int) list[@sexp.opaque])
        * (string option list[@sexp.opaque])
        * (Uuid.t[@sexp.opaque])
    | Local of string option list
  [@@deriving sexp_of]
end

type t =
  | Boolean
  | UInt8
  | UInt16
  | UInt32
  | UInt64
  | Int8
  | Int16
  | Int32
  | Int64
  | Float32
  | Float64
  | Utf8
  | Binary
  | Date
  | Datetime of Time_unit.t * string option
  | Duration of Time_unit.t
  | Time
  | List of t
  | Null
  | Categorical of Rev_mapping.t option
  | Unknown
[@@deriving sexp_of]
