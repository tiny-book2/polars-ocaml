use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse::Parser, parse_macro_input, punctuated::Punctuated};

struct MacroArgs {
    raise_on_err: bool,
}

// TODO: currently, the macro panicks all over the place which is not very nice.
// We should instead emit compile_error! with the appropriate error messages.

// TODO: a common mistake when using the attribute macro is to specify OCaml<_>
// for arguments or OCamlRef<_> for return types, which should never happen.
// In these cases, the macro should probably point out this issue and suggest
// what to do (use the other type).

// When `raise_on_err` is true, the macro will expect the function to return
// `Result<OCaml<_>, String>` and will raise an OCaml exception if the function
// returns an error.
fn ocaml_interop_export_implementation(item_fn: syn::ItemFn, args: MacroArgs) -> TokenStream2 {
    let mut inputs_iter = item_fn.sig.inputs.iter().map(|fn_arg| match fn_arg {
        syn::FnArg::Receiver(_) => panic!("receiver not supported"),
        syn::FnArg::Typed(pat_type) => pat_type.clone(),
    });

    // The first argument to the function corresponds to the OCaml runtime.
    let runtime_name = match *inputs_iter.next().unwrap().pat {
        syn::Pat::Ident(pat_ident) => pat_ident.ident,
        _ => panic!("expected ident"),
    };

    // The remaining arguments are stripped of their types and converted to
    // `RawOCaml` values.
    let new_inputs: Punctuated<_, _> = inputs_iter
        .clone()
        .map(|pat_type| {
            syn::FnArg::Typed(syn::PatType {
                ty: syn::parse2(quote! {
                    ::ocaml_interop::RawOCaml
                })
                .unwrap(),
                ..pat_type
            })
        })
        .collect();
    let number_of_arguments = new_inputs.len();

    let signature = syn::Signature {
        inputs: new_inputs,
        output: syn::parse2(quote! {
            -> ::ocaml_interop::RawOCaml
        })
        .unwrap(),
        ..item_fn.sig.clone()
    };

    // We take each non-runtime argument to the function and convert them to the
    // appropriate Rust type.
    let locals = inputs_iter.map(|pat_type| match *pat_type.pat {
        syn::Pat::Ident(pat_ident) => {
            let ident = pat_ident.ident;
            let ty = pat_type.ty;
            quote! {
                let #ident: #ty = &::ocaml_interop::BoxRoot::new(unsafe {
                    OCaml::new(cr, #ident)
                });
            }
        }
        _ => panic!("expected ident"),
    });

    let return_type = match item_fn.sig.output.clone() {
        syn::ReturnType::Default => panic!("functions with no return type are not supported"),
        syn::ReturnType::Type(_, ty) => ty,
    };
    let block = item_fn.block.clone();

    let native_function = if !args.raise_on_err {
        quote! {
            #[no_mangle]
            pub extern "C" #signature {
                match ::std::panic::catch_unwind(|| {
                    let #runtime_name = unsafe {
                        &mut ::ocaml_interop::OCamlRuntime::recover_handle()
                    };

                    #( #locals )*

                    {
                        let return_value: #return_type = #block;

                        unsafe { return_value.raw() }
                    }
                }) {
                    Ok(value) => value,
                    Err(cause) => {
                        // This is only safe if the runtime lock is held, which
                        // *won't* be the case if any Rust code panics while we have
                        // given up the runtime lock. I think when we start adding
                        // code that does this we'll need some thread local variable
                        // that keeps track of whether we have the runtime lock or
                        // not (since prior to OCaml 5 there's not built-in way to
                        // keep track of this[1]).
                        //
                        // [1]: https://github.com/ocaml/ocaml/issues/5299
                        //
                        // After further discussion, I think this is safe (as long
                        // as we use OCamlRuntime::releasing_runtime) since the lock
                        // should almost always be re-acquired on the event of a panic.
                        let cr = unsafe { &mut ::ocaml_interop::OCamlRuntime::recover_handle() };
                        unsafe { raise_ocaml_exception_from_panic(cr, cause) }
                    },
                }
            }
        }
    } else {
        quote! {
            #[no_mangle]
            pub extern "C" #signature {
                match ::std::panic::catch_unwind(|| {
                    let #runtime_name = unsafe {
                        &mut ::ocaml_interop::OCamlRuntime::recover_handle()
                    };

                    #( #locals )*

                    {
                        let return_value: #return_type = #block;

                        Ok(unsafe { return_value.raw() })
                    }
                }) {
                    Ok(Ok(value)) => value,
                    Ok(Err(error)) => {
                        let cr = unsafe { &mut ::ocaml_interop::OCamlRuntime::recover_handle() };
                        unsafe { raise_ocaml_exception(cr, error) }
                    },
                    Err(cause) => {
                        let cr = unsafe { &mut ::ocaml_interop::OCamlRuntime::recover_handle() };
                        unsafe { raise_ocaml_exception_from_panic(cr, cause) }
                    },
                }
            }
        }
    };

    // We need to generate different functions for the bytecode and native
    // versions of the function if there is more than a certain number of arguments.
    // See https://v2.ocaml.org/manual/intfc.html#ss:c-prim-impl for details.
    if number_of_arguments > 5 {
        let native_function_name = item_fn.sig.ident;

        let bytecode_function_name = syn::Ident::new(
            &format!("{}_bytecode", native_function_name),
            Span::call_site(),
        );

        let arguments = (0..number_of_arguments).map(|i| {
            quote! {
                argv[#i]
            }
        });

        quote! {
            #native_function

            #[no_mangle]
            pub extern "C" fn #bytecode_function_name(
            argv: *const ::ocaml_interop::RawOCaml,
            argn: isize,
            ) -> ::ocaml_interop::RawOCaml {
                if argn as usize != #number_of_arguments {
                    panic!("expected {} arguments, got {}", #number_of_arguments, argn);
                }

                let argv = unsafe { ::std::slice::from_raw_parts(argv, argn as usize) };

                #native_function_name(#( #arguments ),*)
            }
        }
    } else {
        native_function
    }
}

#[proc_macro_attribute]
pub fn ocaml_interop_export(args: TokenStream, annotated_item: TokenStream) -> TokenStream {
    let args = syn::punctuated::Punctuated::<syn::Ident, syn::Token![,]>::parse_terminated
        .parse(args)
        .unwrap()
        .into_iter()
        .map(|ident| format!("{}", ident))
        .collect::<Vec<_>>();
    let item_fn = parse_macro_input!(annotated_item as syn::ItemFn);

    let mut macro_args = MacroArgs {
        raise_on_err: false,
    };
    for arg in &args {
        match arg.as_str() {
            "raise_on_err" => macro_args.raise_on_err = true,
            _ => panic!(
                "unexpected argument in ocaml_interop_export: {} in {:?}",
                arg, args
            ),
        }
    }

    let expanded = ocaml_interop_export_implementation(item_fn, macro_args);

    TokenStream::from(expanded)
}

// TODO: if below code ever changes, consider extracting below code into a
// separate create and freezing the major version, as suggested in:
// https://github.com/mt-caret/polars-ocaml/pull/35#discussion_r1285659995

// `std::panic::catch_unwind` only stores the error string passed to the panic,
// which means that we can't figure out the backtrace by the time we see the Err
// case. So, we use `std::panic::set_hook` to store the backtrace in a thread
// local variable to be retrieved when the unwind is caught as discussed here:
// https://stackoverflow.com/a/73711057
#[proc_macro]
pub fn ocaml_interop_backtrace_support(_item: TokenStream) -> TokenStream {
    let expanded = quote! {
        thread_local! {
            static LAST_BACKTRACE: ::std::cell::Cell<::std::option::Option<::std::backtrace::Backtrace>> =
                const { ::std::cell::Cell::new(::std::option::Option::None) };
        }

        #[::polars_ocaml_macros::ocaml_interop_export]
        fn rust_record_panic_backtraces(
            cr: &mut &mut ::ocaml_interop::OCamlRuntime,
            unit: ::ocaml_interop::OCamlRef<()>
        ) -> ::ocaml_interop::OCaml<()> {
            let () = unit.to_rust(cr);

            // TODO: once update_hook stabilizes, use that instead of take_hook/set_hook:
            // https://github.com/rust-lang/rust/issues/92649
            let last_hook = ::std::panic::take_hook();
            ::std::panic::set_hook(::std::boxed::Box::new(move |panic_info| {
                let trace = ::std::backtrace::Backtrace::force_capture();
                LAST_BACKTRACE.with(|last_backtrace| {
                    last_backtrace.set(::std::option::Option::Some(trace));
                });


                last_hook(panic_info);
            }));

            ::ocaml_interop::OCaml::unit()
        }

        #[::polars_ocaml_macros::ocaml_interop_export]
        fn rust_clear_panic_hook(
            cr: &mut &mut ::ocaml_interop::OCamlRuntime,
            unit: ::ocaml_interop::OCamlRef<()>
        ) -> ::ocaml_interop::OCaml<()> {
            let () = unit.to_rust(cr);

            ::std::panic::set_hook(::std::boxed::Box::new(|_panic_info| ()));

            ::ocaml_interop::OCaml::unit()
        }

        // Note that OCaml exceptions will jump directly back into OCaml code
        // without unwinding Rust code, so you *must* ensure that you don't have
        // any un-dropped (non-OCaml) Rust values around when you call this
        // function (or drop() will never be called for them).
        pub unsafe fn raise_ocaml_exception(
            cr: &mut &mut ::ocaml_interop::OCamlRuntime,
            cause: String
        ) -> ! {
            let error_message = {
                let last_backtrace = LAST_BACKTRACE.with(|last_backtrace| last_backtrace.take());

                let error_message = match last_backtrace {
                    None => format!("Polars panicked: {}\nbacktrace not captured", cause),
                    Some(last_backtrace) => {
                        format!("Polars panicked: {}\nBacktrace:\n{}", cause, last_backtrace)
                    }
                };

                let error_message: OCaml<String> = error_message.to_ocaml(cr);
                unsafe { error_message.raw() }
            };

            // We need to drop `cause`, but `error_message` is fine, since it's
            // an OCaml value which will be
            // garbage-collected by the OCaml runtime.
            drop(cause);

            unsafe {
                ::ocaml_sys::caml_failwith_value(error_message);
            }

            unreachable!("caml_failwith should never return")
        }

        pub unsafe fn raise_ocaml_exception_from_panic(
            cr: &mut &mut ::ocaml_interop::OCamlRuntime,
            cause: Box<dyn ::core::any::Any + Send>
        ) -> ! {
            let error_message =
                if let Some(cause) = cause.downcast_ref::<&str>() {
                    cause.to_string()
                } else if let Some(cause) = cause.downcast_ref::<String>() {
                    cause.to_string()
                } else {
                    format!("{:?}", cause)
                };

            drop(cause);

            raise_ocaml_exception(cr, error_message)
        }
    };

    TokenStream::from(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::expect;
    use proc_macro2::TokenStream as TokenStream2;

    fn pretty_print_item(item: &TokenStream2) -> String {
        let file: syn::File = syn::parse2(item.clone()).unwrap();

        prettyplease::unparse(&file)
    }

    fn apply_macro_and_pretty_print(input: TokenStream2, args: MacroArgs) -> String {
        let item_fn = syn::parse2(input).unwrap();
        let expanded = ocaml_interop_export_implementation(item_fn, args);
        pretty_print_item(&expanded)
    }

    #[test]
    fn test_simple_function() {
        let macro_output = apply_macro_and_pretty_print(
            quote! {
                fn rust_expr_col(
                    cr: &mut &mut OCamlRuntime,
                    name: OCamlRef<String>
                ) -> OCaml<DynBox<Expr>> {
                    let name: String = name.to_rust(cr);
                    OCaml::box_value(cr, col(&name))
                }
            },
            MacroArgs {
                raise_on_err: false,
            },
        );

        expect![[r##"
            #[no_mangle]
            pub extern "C" fn rust_expr_col(
                name: ::ocaml_interop::RawOCaml,
            ) -> ::ocaml_interop::RawOCaml {
                match ::std::panic::catch_unwind(|| {
                    let cr = unsafe { &mut ::ocaml_interop::OCamlRuntime::recover_handle() };
                    let name: OCamlRef<String> = &::ocaml_interop::BoxRoot::new(unsafe {
                        OCaml::new(cr, name)
                    });
                    {
                        let return_value: OCaml<DynBox<Expr>> = {
                            let name: String = name.to_rust(cr);
                            OCaml::box_value(cr, col(&name))
                        };
                        unsafe { return_value.raw() }
                    }
                }) {
                    Ok(value) => value,
                    Err(cause) => {
                        let cr = unsafe { &mut ::ocaml_interop::OCamlRuntime::recover_handle() };
                        unsafe { raise_ocaml_exception_from_panic(cr, cause) }
                    }
                }
            }
        "##]]
        .assert_eq(&macro_output);

        let macro_output = apply_macro_and_pretty_print(
            quote! {
                fn rust_expr_col(
                    cr: &mut &mut OCamlRuntime,
                    name: OCamlRef<String>
                ) -> OCaml<DynBox<Expr>> {
                    let name: String = name.to_rust(cr);
                    OCaml::box_value(cr, col(&name))
                }
            },
            MacroArgs { raise_on_err: true },
        );

        expect![[r##"
            #[no_mangle]
            pub extern "C" fn rust_expr_col(
                name: ::ocaml_interop::RawOCaml,
            ) -> ::ocaml_interop::RawOCaml {
                match ::std::panic::catch_unwind(|| {
                    let cr = unsafe { &mut ::ocaml_interop::OCamlRuntime::recover_handle() };
                    let name: OCamlRef<String> = &::ocaml_interop::BoxRoot::new(unsafe {
                        OCaml::new(cr, name)
                    });
                    {
                        let return_value: OCaml<DynBox<Expr>> = {
                            let name: String = name.to_rust(cr);
                            OCaml::box_value(cr, col(&name))
                        };
                        Ok(unsafe { return_value.raw() })
                    }
                }) {
                    Ok(Ok(value)) => value,
                    Ok(Err(error)) => {
                        let cr = unsafe { &mut ::ocaml_interop::OCamlRuntime::recover_handle() };
                        unsafe { raise_ocaml_exception(cr, error) }
                    }
                    Err(cause) => {
                        let cr = unsafe { &mut ::ocaml_interop::OCamlRuntime::recover_handle() };
                        unsafe { raise_ocaml_exception_from_panic(cr, cause) }
                    }
                }
            }
        "##]]
        .assert_eq(&macro_output);
    }

    #[test]
    fn test_bytecode_generation() {
        let macro_output = apply_macro_and_pretty_print(
            quote! {
                fn rust_expr_sample_n(
                    cr: &mut &mut OCamlRuntime,
                    expr: OCamlRef<DynBox<Expr>>,
                    n: OCamlRef<OCamlInt>,
                    with_replacement: OCamlRef<bool>,
                    shuffle: OCamlRef<bool>,
                    seed: OCamlRef<Option<OCamlInt>>,
                    fixed_seed: OCamlRef<bool>,
                ) -> OCaml<DynBox<Expr>> {
                    let Abstract(expr) = expr.to_rust(cr);
                    let n = n.to_rust::<Coerce<_, i64, usize>>(cr).get()?;
                    let with_replacement: bool = with_replacement.to_rust(cr);
                    let shuffle: bool = shuffle.to_rust(cr);
                    let seed = seed
                        .to_rust::<Coerce<_, Option<i64>, Option<u64>>>(cr)
                        .get()?;
                    let fixed_seed = fixed_seed.to_rust(cr);

                    Abstract(expr.sample_n(n, with_replacement, shuffle, seed, fixed_seed)).to_ocaml(cr)
                }
            },
            MacroArgs { raise_on_err: true },
        );

        expect![[r##"
            #[no_mangle]
            pub extern "C" fn rust_expr_sample_n(
                expr: ::ocaml_interop::RawOCaml,
                n: ::ocaml_interop::RawOCaml,
                with_replacement: ::ocaml_interop::RawOCaml,
                shuffle: ::ocaml_interop::RawOCaml,
                seed: ::ocaml_interop::RawOCaml,
                fixed_seed: ::ocaml_interop::RawOCaml,
            ) -> ::ocaml_interop::RawOCaml {
                match ::std::panic::catch_unwind(|| {
                    let cr = unsafe { &mut ::ocaml_interop::OCamlRuntime::recover_handle() };
                    let expr: OCamlRef<DynBox<Expr>> = &::ocaml_interop::BoxRoot::new(unsafe {
                        OCaml::new(cr, expr)
                    });
                    let n: OCamlRef<OCamlInt> = &::ocaml_interop::BoxRoot::new(unsafe {
                        OCaml::new(cr, n)
                    });
                    let with_replacement: OCamlRef<bool> = &::ocaml_interop::BoxRoot::new(unsafe {
                        OCaml::new(cr, with_replacement)
                    });
                    let shuffle: OCamlRef<bool> = &::ocaml_interop::BoxRoot::new(unsafe {
                        OCaml::new(cr, shuffle)
                    });
                    let seed: OCamlRef<Option<OCamlInt>> = &::ocaml_interop::BoxRoot::new(unsafe {
                        OCaml::new(cr, seed)
                    });
                    let fixed_seed: OCamlRef<bool> = &::ocaml_interop::BoxRoot::new(unsafe {
                        OCaml::new(cr, fixed_seed)
                    });
                    {
                        let return_value: OCaml<DynBox<Expr>> = {
                            let Abstract(expr) = expr.to_rust(cr);
                            let n = n.to_rust::<Coerce<_, i64, usize>>(cr).get()?;
                            let with_replacement: bool = with_replacement.to_rust(cr);
                            let shuffle: bool = shuffle.to_rust(cr);
                            let seed = seed
                                .to_rust::<Coerce<_, Option<i64>, Option<u64>>>(cr)
                                .get()?;
                            let fixed_seed = fixed_seed.to_rust(cr);
                            Abstract(expr.sample_n(n, with_replacement, shuffle, seed, fixed_seed))
                                .to_ocaml(cr)
                        };
                        Ok(unsafe { return_value.raw() })
                    }
                }) {
                    Ok(Ok(value)) => value,
                    Ok(Err(error)) => {
                        let cr = unsafe { &mut ::ocaml_interop::OCamlRuntime::recover_handle() };
                        unsafe { raise_ocaml_exception(cr, error) }
                    }
                    Err(cause) => {
                        let cr = unsafe { &mut ::ocaml_interop::OCamlRuntime::recover_handle() };
                        unsafe { raise_ocaml_exception_from_panic(cr, cause) }
                    }
                }
            }
            #[no_mangle]
            pub extern "C" fn rust_expr_sample_n_bytecode(
                argv: *const ::ocaml_interop::RawOCaml,
                argn: isize,
            ) -> ::ocaml_interop::RawOCaml {
                if argn as usize != 6usize {
                    panic!("expected {} arguments, got {}", 6usize, argn);
                }
                let argv = unsafe { ::std::slice::from_raw_parts(argv, argn as usize) };
                rust_expr_sample_n(
                    argv[0usize],
                    argv[1usize],
                    argv[2usize],
                    argv[3usize],
                    argv[4usize],
                    argv[5usize],
                )
            }
        "##]]
        .assert_eq(&macro_output);
    }
}
