(lang dune 3.8)

(name polars)

(generate_opam_files true)

(source
 (github mt-caret/polars-ocaml))

(authors "mtakeda")

(maintainers "mtakeda")

(license MIT)

; TODO: update below link once we start generating documentation
; (https://github.com/mt-caret/polars-ocaml/issues/5)

(documentation https://github.com/mt-caret/polars-ocaml)

(using mdx 0.4)

(package
 (name polars)
 (synopsis "OCaml bindings to the Polars dataframe library")
 (description "OCaml bindings to the Polars dataframe library")
 (depends
  (core_bench :with-test)
  (expect_test_helpers_core :with-test)
  (mdx :with-test)
  (ocamlformat :dev)
  (odoc :with-doc)
  (re2 :with-test)
  (shell :with-test)
  core
  core_kernel
  dune
  ocaml
  ppx_jane
  ppx_typed_fields)
 (tags
  (data-science polars rust)))

(package
 (name polars_async)
 (synopsis "Async OCaml bindings to the Polars dataframe library")
 (description "Async OCaml bindings to the Polars dataframe library")
 (depends polars async core)
 (tags
  (data-science polars rust)))
