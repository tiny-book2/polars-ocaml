use arrow2::array::Utf8Array;
use ocaml_interop::{
    ocaml_alloc_tagged_block, ocaml_alloc_variant, ocaml_unpack_variant, DynBox, FromOCaml, OCaml,
    OCamlInt, OCamlList, OCamlRuntime, ToOCaml,
};
use polars::prelude::*;
use std::borrow::Borrow;

unsafe fn ocaml_failwith(error_message: &str) -> ! {
    let error_message = std::ffi::CString::new(error_message).expect("CString::new failed");
    unsafe {
        ocaml_sys::caml_failwith(error_message.as_ptr());
    }
    unreachable!("ocaml_failwith should never return")
}

pub struct Abstract<T>(pub T);
unsafe impl<T: 'static + Clone> FromOCaml<DynBox<T>> for Abstract<T> {
    fn from_ocaml(v: OCaml<DynBox<T>>) -> Self {
        Abstract(Borrow::<T>::borrow(&v).clone())
    }
}

unsafe impl<T: 'static + Clone> ToOCaml<DynBox<T>> for Abstract<T> {
    fn to_ocaml<'a>(&self, cr: &'a mut OCamlRuntime) -> OCaml<'a, DynBox<T>> {
        // TODO: I don't fully understand why ToOCaml takes a &self, since that
        // prevents us from using box_value without a clone() call.
        OCaml::box_value(cr, self.0.clone())
    }
}

pub fn unwrap_abstract_vec<T>(v: Vec<Abstract<T>>) -> Vec<T> {
    v.into_iter().map(|Abstract(v)| v).collect()
}

pub struct PolarsTimeUnit(pub TimeUnit);

unsafe impl FromOCaml<TimeUnit> for PolarsTimeUnit {
    fn from_ocaml(v: OCaml<TimeUnit>) -> Self {
        let result = ocaml_unpack_variant! {
            v => {
                TimeUnit::Nanoseconds,
                TimeUnit::Microseconds,
                TimeUnit::Milliseconds,
            }
        };
        PolarsTimeUnit(result.expect("Failure when unpacking an OCaml<TimeUnit> variant into PolarsTimeUnit (unexpected tag value"))
    }
}

unsafe impl ToOCaml<TimeUnit> for PolarsTimeUnit {
    fn to_ocaml<'a>(&self, cr: &'a mut OCamlRuntime) -> OCaml<'a, TimeUnit> {
        let PolarsTimeUnit(timeunit) = self;
        ocaml_alloc_variant! {
            cr, timeunit => {
                TimeUnit::Nanoseconds,
                TimeUnit::Microseconds,
                TimeUnit::Milliseconds,
            }
        }
    }
}

pub struct PolarsRevMapping(pub RevMapping);

unsafe impl FromOCaml<RevMapping> for PolarsRevMapping {
    fn from_ocaml(v: OCaml<RevMapping>) -> Self {
        let result = ocaml_unpack_variant! {
            v => {
                RevMapping::Global(map: OCamlList<(OCamlInt,OCamlInt)>, cache: OCamlList<Option<String>>, uuid: DynBox<u128>) => {
                    let map: Vec<(i64,i64)> = map;
                    let map: Option<Vec<(u32, u32)>> = map.into_iter().map(|(k, v)| Some((k.try_into().ok()?, v.try_into().ok()?))).collect();
                    let map: Option<PlHashMap<u32, u32>> = map.map(|vec| vec.into_iter().collect());
                     match map {
                         None => unsafe { ocaml_failwith("Failure when translating an PolarsRevMapping") },
                         Some(map) => {
                            let cache: Vec<Option<String>> = cache;
                            let cache = Utf8Array::<i64>::from(cache);
                            let Abstract(uuid) = uuid;
                            RevMapping::Global(map, cache, uuid)
                         }
                     }
                },
                RevMapping::Local(cache: OCamlList<Option<String>>) => {
                    let cache: Vec<Option<String>> = cache;
                    let cache = Utf8Array::<i64>::from(cache);
                    RevMapping::Local(cache)
                }
            }
        };
        PolarsRevMapping(result.expect("Failure when unpacking an OCaml<RevMapping> variant into PolarsRevMapping (unexpected tag value"))
    }
}

unsafe impl ToOCaml<RevMapping> for PolarsRevMapping {
    fn to_ocaml<'a>(&self, cr: &'a mut OCamlRuntime) -> OCaml<'a, RevMapping> {
        let PolarsRevMapping(rev_mapping) = self;
        unsafe {
            match rev_mapping {
                RevMapping::Global(map, cache, uuid) => {
                    let map: Vec<(i64, i64)> =
                        map.iter().map(|(k, v)| (*k as i64, *v as i64)).collect();
                    let cache: Vec<Option<String>> = cache
                        .into_iter()
                        .map(|str| str.map(|str| str.to_string()))
                        .collect();
                    let uuid = Abstract(uuid.clone());
                    ocaml_alloc_tagged_block!(cr, 0, map: OCamlList<(OCamlInt, OCamlInt)>, cache: OCamlList<Option<String>>, uuid: DynBox<u128>)
                }
                RevMapping::Local(cache) => {
                    let cache: Vec<Option<String>> = cache
                        .into_iter()
                        .map(|str| str.map(|str| str.to_string()))
                        .collect();
                    ocaml_alloc_tagged_block!(cr, 1, cache: OCamlList<Option<String>>)
                }
            }
        }
    }
}

pub struct PolarsDataType(pub DataType);

unsafe impl FromOCaml<DataType> for PolarsDataType {
    fn from_ocaml(v: OCaml<DataType>) -> Self {
        let result = ocaml_unpack_variant! {
            v => {
                DataType::Boolean,
                DataType::UInt8,
                DataType::UInt16,
                DataType::UInt32,
                DataType::UInt64,
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::Float32,
                DataType::Float64,
                DataType::Utf8,
                DataType::Binary,
                DataType::Date,
                DataType::Datetime(timeunit: TimeUnit, timezone: Option<String>) => {
                    let PolarsTimeUnit(timeunit) = timeunit;
                    DataType::Datetime(timeunit, timezone)},
                DataType::Duration(timeunit: TimeUnit) => {
                    let PolarsTimeUnit(timeunit) = timeunit;
                    DataType::Duration(timeunit)},
                DataType::Time,
                DataType::List(datatype: DataType) => {
                    let PolarsDataType(datatype) = datatype;
                    DataType::List(Box::new(datatype))
                },
                DataType::Null,
                DataType::Categorical(rev_mapping: Option<RevMapping>) => {
                    let rev_mapping: Option<PolarsRevMapping> = rev_mapping;
                    let rev_mapping = rev_mapping.map(|PolarsRevMapping(rev_mapping)| Arc::new(rev_mapping));
                    DataType::Categorical(rev_mapping)
                },
                DataType::Unknown,
            }
        };
        PolarsDataType(result.expect("Failure when unpacking an OCaml<DataType> variant into PolarsDataType (unexpected tag value"))
    }
}

unsafe fn ocaml_value<'a, T>(cr: &'a mut OCamlRuntime, n: i32) -> OCaml<'a, T> {
    unsafe { OCaml::new(cr, OCaml::of_i32(n).raw()) }
}

unsafe impl ToOCaml<DataType> for PolarsDataType {
    fn to_ocaml<'a>(&self, cr: &'a mut OCamlRuntime) -> OCaml<'a, DataType> {
        let PolarsDataType(datatype) = self;
        // We expand out the macro here since we need to do some massaging of the
        // values to get things into the right shape to convert to OCaml values
        unsafe {
            match datatype {
                DataType::Boolean => ocaml_value(cr, 0),
                DataType::UInt8 => ocaml_value(cr, 1),
                DataType::UInt16 => ocaml_value(cr, 2),
                DataType::UInt32 => ocaml_value(cr, 3),
                DataType::UInt64 => ocaml_value(cr, 4),
                DataType::Int8 => ocaml_value(cr, 5),
                DataType::Int16 => ocaml_value(cr, 6),
                DataType::Int32 => ocaml_value(cr, 7),
                DataType::Int64 => ocaml_value(cr, 8),
                DataType::Float32 => ocaml_value(cr, 9),
                DataType::Float64 => ocaml_value(cr, 10),
                DataType::Utf8 => ocaml_value(cr, 11),
                DataType::Binary => ocaml_value(cr, 12),
                DataType::Date => ocaml_value(cr, 13),
                DataType::Datetime(timeunit, timezone) => {
                    let timeunit = PolarsTimeUnit(*timeunit);
                    let timezone = timezone.clone();
                    ocaml_alloc_tagged_block!(cr, 0, timeunit : TimeUnit, timezone: Option<String>)
                }
                DataType::Duration(timeunit) => {
                    let timeunit = PolarsTimeUnit(*timeunit);
                    ocaml_alloc_tagged_block!(cr, 1,  timeunit: TimeUnit)
                }
                DataType::Time => ocaml_value(cr, 14),
                DataType::List(datatype) => {
                    let datatype = PolarsDataType(*datatype.clone());
                    ocaml_alloc_tagged_block!(cr, 2,  datatype: DataType)
                }
                DataType::Null => ocaml_value(cr, 15),
                DataType::Categorical(rev_mapping) => {
                    let rev_mapping = rev_mapping
                        .clone()
                        .map(|rev_mapping| PolarsRevMapping(rev_mapping.as_ref().clone()));
                    ocaml_alloc_tagged_block!(cr, 3,  rev_mapping: Option<RevMapping>)
                }
                DataType::Unknown => ocaml_value(cr, 16),
            }
        }
    }
}
