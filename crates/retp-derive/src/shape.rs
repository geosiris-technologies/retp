//! What a Rust type says about its Avro encoding.
//!
//! The derive reads the encoding off the **spelling** of each field's type,
//! the way the generator used to read it off the schema: `Vec<f64>` is an
//! `array<double>` and takes the block path (ADR-0004), `Option<T>` is the
//! union of `null` and `T`, and any other path is a named type that
//! implements `Avro` itself.
//!
//! Reading a spelling is only safe if every spelling that could mean two
//! things is refused, and those refusals are the point of this file: a
//! `HashMap` would write its keys in an order that changes between runs, an
//! `u32` has no Avro counterpart, and `Option<Option<T>>` would be a union
//! inside a union, which Avro forbids. Each is a compile error that says so.

use proc_macro2::{Literal, TokenStream};
use quote::quote;
use syn::spanned::Spanned;
use syn::{Expr, GenericArgument, Lit, PathArguments, Type, UnOp};

/// The Avro encoding a field type stands for.
#[derive(Debug, Clone)]
pub enum Shape {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array(Box<Shape>),
    Map(Box<Shape>),
    /// The union of `null` (branch 0) and the inner shape.
    Optional(Box<Shape>),
    /// A record, enumeration, fixed or union that implements `Avro` itself.
    Named(Box<Type>),
}

/// The type arguments of the last path segment, in order.
fn type_arguments(arguments: &PathArguments) -> Vec<&Type> {
    match arguments {
        PathArguments::AngleBracketed(angle) => angle
            .args
            .iter()
            .filter_map(|arg| match arg {
                GenericArgument::Type(ty) => Some(ty),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn is_u8(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.qself.is_none() && path.path.is_ident("u8"))
}

/// Read the encoding off a field type, or refuse it by name.
pub fn shape_of(ty: &Type) -> syn::Result<Shape> {
    match ty {
        Type::Tuple(tuple) if tuple.elems.is_empty() => Ok(Shape::Null),
        Type::Paren(inner) => shape_of(&inner.elem),
        Type::Group(inner) => shape_of(&inner.elem),
        Type::Path(path) if path.qself.is_none() => {
            let last = path
                .path
                .segments
                .last()
                .ok_or_else(|| syn::Error::new(ty.span(), "an empty type path"))?;
            let name = last.ident.to_string();
            let args = type_arguments(&last.arguments);
            match (name.as_str(), args.as_slice()) {
                ("bool", []) => Ok(Shape::Boolean),
                ("i32", []) => Ok(Shape::Int),
                ("i64", []) => Ok(Shape::Long),
                ("f32", []) => Ok(Shape::Float),
                ("f64", []) => Ok(Shape::Double),
                ("String", []) => Ok(Shape::String),
                ("Vec", [item]) if is_u8(item) => Ok(Shape::Bytes),
                ("Vec", [item]) => Ok(Shape::Array(Box::new(shape_of(item)?))),
                ("BTreeMap", [key, value]) => {
                    if !matches!(shape_of(key)?, Shape::String) {
                        return Err(syn::Error::new(
                            key.span(),
                            "an Avro map is keyed by `String`, and by nothing else",
                        ));
                    }
                    Ok(Shape::Map(Box::new(shape_of(value)?)))
                }
                ("Option", [inner]) => match shape_of(inner)? {
                    Shape::Optional(_) | Shape::Null => Err(syn::Error::new(
                        inner.span(),
                        "an `Option` of this has no Avro encoding: it would be a union \
                         directly inside a union, which Avro forbids",
                    )),
                    inner => Ok(Shape::Optional(Box::new(inner))),
                },
                ("HashMap", _) => Err(syn::Error::new(
                    ty.span(),
                    "a `HashMap` would write its entries in an order that changes from run \
                     to run; an Avro map is held in a `BTreeMap`",
                )),
                ("Vec" | "BTreeMap" | "Option", _) => Err(syn::Error::new(
                    ty.span(),
                    format!("`{name}` with these type arguments has no Avro encoding"),
                )),
                (
                    "u8" | "u16" | "u32" | "u64" | "u128" | "usize" | "i8" | "i16" | "i128" | "isize"
                    | "char" | "str",
                    [],
                ) => Err(syn::Error::new(
                    ty.span(),
                    format!(
                        "Avro has no `{name}`: its numbers are `int` (i32), `long` (i64), \
                         `float` (f32) and `double` (f64), and its text is `String`"
                    ),
                )),
                _ => Ok(Shape::Named(Box::new(ty.clone()))),
            }
        }
        _ => Err(syn::Error::new(
            ty.span(),
            "this type has no Avro encoding the derive can read from its spelling; \
             a fixed-size value is a named `struct Name(pub [u8; N])`",
        )),
    }
}

/// The statements that write `value`, an expression of type `&T`.
pub fn write(shape: &Shape, value: TokenStream) -> TokenStream {
    let avro = quote!(::retp_avro);
    match shape {
        Shape::Null => quote! { let _ = #value; out.null(); },
        Shape::Boolean => quote! { out.boolean(*#value); },
        Shape::Int => quote! { out.int(*#value); },
        Shape::Long => quote! { out.long(*#value); },
        Shape::Float => quote! { out.float(*#value); },
        Shape::Double => quote! { out.double(*#value); },
        Shape::Bytes => quote! { out.bytes(#value); },
        Shape::String => quote! { out.string(#value); },
        // The block paths (ADR-0004): a contiguous run of fixed-width
        // little-endian values goes out in one piece.
        Shape::Array(item) if matches!(**item, Shape::Double) => quote! { out.array_f64(#value); },
        Shape::Array(item) if matches!(**item, Shape::Float) => quote! { out.array_f32(#value); },
        Shape::Array(item) => {
            let body = write(item, quote!(item));
            quote! { out.array(#value, |out, item| { #body }); }
        }
        Shape::Map(item) => {
            let body = write(item, quote!(item));
            quote! { out.map(#value, |out, item| { #body }); }
        }
        Shape::Optional(item) => {
            let body = write(item, quote!(item));
            quote! { out.optional(#value, 0, |out, item| { #body }); }
        }
        Shape::Named(ty) => quote! { <#ty as #avro::Avro>::write(#value, out); },
    }
}

/// An expression of type `Result<T>` that reads one value.
///
/// `label` names the place in refusals - `Owner.field` - so that a bad union
/// branch deep in a message says where it was.
pub fn read(shape: &Shape, label: &str) -> TokenStream {
    let avro = quote!(::retp_avro);
    match shape {
        Shape::Null => quote! { ::core::result::Result::Ok(input.null()) },
        Shape::Boolean => quote! { input.boolean() },
        Shape::Int => quote! { input.int() },
        Shape::Long => quote! { input.long() },
        Shape::Float => quote! { input.float() },
        Shape::Double => quote! { input.double() },
        Shape::Bytes => quote! { input.bytes() },
        Shape::String => quote! { input.string() },
        Shape::Array(item) if matches!(**item, Shape::Double) => quote! { input.array_f64() },
        Shape::Array(item) if matches!(**item, Shape::Float) => quote! { input.array_f32() },
        Shape::Array(item) => {
            let body = read(item, label);
            quote! { input.array(|input| #body) }
        }
        Shape::Map(item) => {
            let body = read(item, label);
            quote! { input.map(|input| #body) }
        }
        Shape::Optional(item) => {
            let body = read(item, label);
            quote! { input.optional(#label, 0, |input| #body) }
        }
        Shape::Named(ty) => quote! { <#ty as #avro::Avro>::read(input) },
    }
}

/// The Avro JSON name of the union branch this shape is: the primitive type
/// name, or the full name of the named type. An expression of type `&str`.
pub fn branch_name(shape: &Shape) -> TokenStream {
    let avro = quote!(::retp_avro);
    match shape {
        Shape::Null => quote!("null"),
        Shape::Boolean => quote!("boolean"),
        Shape::Int => quote!("int"),
        Shape::Long => quote!("long"),
        Shape::Float => quote!("float"),
        Shape::Double => quote!("double"),
        Shape::Bytes => quote!("bytes"),
        Shape::String => quote!("string"),
        Shape::Array(_) => quote!("array"),
        Shape::Map(_) => quote!("map"),
        // `shape_of` never builds a union of a union; this is the unreachable
        // arm, named so that a future change cannot make it quietly wrong.
        Shape::Optional(_) => quote!("union"),
        Shape::Named(ty) => quote!(<#ty as #avro::Avro>::FULL_NAME),
    }
}

/// An expression of type `json::Value` that encodes `value`, an expression of
/// type `&T`, in the Avro JSON encoding.
pub fn to_json(shape: &Shape, value: TokenStream) -> TokenStream {
    let avro = quote!(::retp_avro);
    let json = quote!(::retp_avro::json);
    match shape {
        Shape::Null => quote! { { let _ = #value; #json::Value::Null } },
        Shape::Boolean => quote! { #json::Value::Bool(*#value) },
        Shape::Int | Shape::Long => quote! { #json::Value::from(*#value) },
        Shape::Float => quote! { #json::of_f32(*#value) },
        Shape::Double => quote! { #json::of_f64(*#value) },
        Shape::Bytes => quote! { #json::of_bytes(#value) },
        Shape::String => quote! { #json::Value::String(::std::string::String::clone(#value)) },
        Shape::Array(item) => {
            let body = to_json(item, quote!(item));
            quote! { #json::Value::Array((#value).iter().map(|item| #body).collect()) }
        }
        Shape::Map(item) => {
            let body = to_json(item, quote!(item));
            quote! {
                {
                    let mut entries = #json::Map::new();
                    for (key, item) in (#value).iter() {
                        entries.insert(::std::string::String::clone(key), #body);
                    }
                    #json::Value::Object(entries)
                }
            }
        }
        Shape::Optional(item) => {
            let name = branch_name(item);
            let body = to_json(item, quote!(item));
            quote! {
                match #value {
                    ::core::option::Option::None => #json::Value::Null,
                    ::core::option::Option::Some(item) => #json::tagged(#name, #body),
                }
            }
        }
        Shape::Named(ty) => quote! { <#ty as #avro::Avro>::to_json(#value) },
    }
}

/// An expression of type `Result<T>` that reads one value from `value`, an
/// expression of type `&json::Value`. `label` names the place in refusals.
pub fn from_json(shape: &Shape, value: TokenStream, label: &str) -> TokenStream {
    let avro = quote!(::retp_avro);
    let json = quote!(::retp_avro::json);
    match shape {
        Shape::Null => quote! { #json::as_null(#value, #label) },
        Shape::Boolean => quote! { #json::as_bool(#value, #label) },
        Shape::Int => quote! { #json::as_i32(#value, #label) },
        Shape::Long => quote! { #json::as_i64(#value, #label) },
        Shape::Float => quote! { #json::as_f32(#value, #label) },
        Shape::Double => quote! { #json::as_f64(#value, #label) },
        Shape::Bytes => quote! { #json::as_bytes(#value, #label) },
        Shape::String => quote! { #json::as_string(#value, #label) },
        Shape::Array(item) => {
            let body = from_json(item, quote!(item), label);
            quote! { #json::as_array(#value, #label, |item| #body) }
        }
        Shape::Map(item) => {
            let body = from_json(item, quote!(item), label);
            quote! { #json::as_map(#value, #label, |item| #body) }
        }
        Shape::Optional(item) => {
            let name = branch_name(item);
            let body = from_json(item, quote!(item), label);
            quote! { #json::optional(#value, #label, #name, |item| #body) }
        }
        Shape::Named(ty) => quote! { <#ty as #avro::Avro>::from_json(#value) },
    }
}

/// The Rust value of a schema default, written the way the schema writes it.
///
/// The attribute holds what the `.avsc` says - `"xml"`, `[]`, `{}`, `null`,
/// `false`, `0` - and this turns it into a value of the field's type. A named
/// type takes a variant: `default = DateTime` is `<IndexKind>::DateTime`.
pub fn default_value(shape: &Shape, expr: &Expr) -> syn::Result<TokenStream> {
    let wrong = |what: &str| syn::Error::new(expr.span(), format!("a default for {what} is {}", hint(shape)));
    match shape {
        Shape::Optional(inner) => {
            if is_ident(expr, "null") {
                Ok(quote!(::core::option::Option::None))
            } else {
                let inner = default_value(inner, expr)?;
                Ok(quote!(::core::option::Option::Some(#inner)))
            }
        }
        Shape::Null if is_ident(expr, "null") => Ok(quote!(())),
        Shape::Null => Err(wrong("`null`")),
        Shape::Boolean => match literal(expr) {
            Some((Lit::Bool(b), false)) => Ok(quote!(#b)),
            _ => Err(wrong("a `boolean`")),
        },
        Shape::Int | Shape::Long => {
            let Some((Lit::Int(int), negative)) = literal(expr) else {
                return Err(wrong("an integer"));
            };
            let magnitude: i128 = int.base10_parse()?;
            let value = if negative { -magnitude } else { magnitude };
            if matches!(shape, Shape::Int) {
                let value = i32::try_from(value)
                    .map_err(|_| syn::Error::new(expr.span(), "out of range for an Avro `int`"))?;
                Ok(literal_tokens(Literal::i32_suffixed(value)))
            } else {
                let value = i64::try_from(value)
                    .map_err(|_| syn::Error::new(expr.span(), "out of range for an Avro `long`"))?;
                Ok(literal_tokens(Literal::i64_suffixed(value)))
            }
        }
        Shape::Float | Shape::Double => {
            let value = match literal(expr) {
                Some((Lit::Int(int), negative)) => {
                    let v: f64 = int.base10_parse()?;
                    if negative { -v } else { v }
                }
                Some((Lit::Float(float), negative)) => {
                    let v: f64 = float.base10_parse()?;
                    if negative { -v } else { v }
                }
                _ => return Err(wrong("a floating-point number")),
            };
            if matches!(shape, Shape::Float) {
                Ok(literal_tokens(Literal::f32_suffixed(value as f32)))
            } else {
                Ok(literal_tokens(Literal::f64_suffixed(value)))
            }
        }
        Shape::String => match literal(expr) {
            Some((Lit::Str(text), false)) => Ok(quote!(::std::string::String::from(#text))),
            _ => Err(wrong("a `string`")),
        },
        // Avro writes a `bytes` default as a JSON string whose code points
        // are the byte values, 0 to 255.
        Shape::Bytes => match literal(expr) {
            Some((Lit::Str(text), false)) => {
                let mut bytes = Vec::new();
                for c in text.value().chars() {
                    let byte = u8::try_from(u32::from(c)).map_err(|_| {
                        syn::Error::new(
                            text.span(),
                            "an Avro `bytes` default is a string of code points 0 to 255",
                        )
                    })?;
                    bytes.push(byte);
                }
                Ok(quote!(::std::vec![#(#bytes),*]))
            }
            _ => Err(wrong("`bytes`")),
        },
        Shape::Array(item) => match expr {
            Expr::Array(array) => {
                let items = array
                    .elems
                    .iter()
                    .map(|e| default_value(item, e))
                    .collect::<syn::Result<Vec<_>>>()?;
                Ok(quote!(::std::vec![#(#items),*]))
            }
            _ => Err(wrong("an `array`")),
        },
        Shape::Map(_) => match expr {
            Expr::Block(block) if block.block.stmts.is_empty() => {
                Ok(quote!(::std::collections::BTreeMap::new()))
            }
            _ => Err(wrong("a `map`")),
        },
        Shape::Named(ty) => match expr {
            Expr::Path(path) if path.qself.is_none() && path.path.get_ident().is_some() => {
                let variant = path.path.get_ident();
                Ok(quote!(<#ty>::#variant))
            }
            _ => Err(wrong("a named type")),
        },
    }
}

fn hint(shape: &Shape) -> &'static str {
    match shape {
        Shape::Null => "`null`",
        Shape::Boolean => "`true` or `false`",
        Shape::Int | Shape::Long => "an integer literal",
        Shape::Float | Shape::Double => "a number literal",
        Shape::String => "a string literal",
        Shape::Bytes => "a string literal of code points 0 to 255",
        Shape::Array(_) => "an array literal, `[...]`",
        Shape::Map(_) => "`{}`: only the empty map has a spelling here",
        Shape::Optional(_) => "`null`, or a default for the inner type",
        Shape::Named(_) => "the name of one of its variants",
    }
}

fn is_ident(expr: &Expr, name: &str) -> bool {
    matches!(expr, Expr::Path(path) if path.qself.is_none() && path.path.is_ident(name))
}

/// A literal, and whether it was negated.
fn literal(expr: &Expr) -> Option<(Lit, bool)> {
    match expr {
        Expr::Lit(lit) => Some((lit.lit.clone(), false)),
        Expr::Unary(unary) if matches!(unary.op, UnOp::Neg(_)) => match &*unary.expr {
            Expr::Lit(lit) => Some((lit.lit.clone(), true)),
            _ => None,
        },
        Expr::Group(group) => literal(&group.expr),
        Expr::Paren(paren) => literal(&paren.expr),
        _ => None,
    }
}

fn literal_tokens(literal: Literal) -> TokenStream {
    quote!(#literal)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(text: &str) -> syn::Result<Shape> {
        shape_of(&syn::parse_str::<Type>(text).expect("a type"))
    }

    fn refusal(text: &str) -> String {
        shape(text).expect_err("refused").to_string()
    }

    #[test]
    fn every_primitive_is_read_off_its_spelling() {
        assert!(matches!(shape("bool").unwrap(), Shape::Boolean));
        assert!(matches!(shape("i32").unwrap(), Shape::Int));
        assert!(matches!(shape("i64").unwrap(), Shape::Long));
        assert!(matches!(shape("f32").unwrap(), Shape::Float));
        assert!(matches!(shape("f64").unwrap(), Shape::Double));
        assert!(matches!(shape("String").unwrap(), Shape::String));
        assert!(matches!(shape("()").unwrap(), Shape::Null));
    }

    #[test]
    fn a_vec_of_u8_is_bytes_and_not_an_array() {
        assert!(matches!(shape("Vec<u8>").unwrap(), Shape::Bytes));
        assert!(matches!(shape("Vec<i32>").unwrap(), Shape::Array(item) if matches!(*item, Shape::Int)));
    }

    #[test]
    fn a_full_path_reads_like_a_short_one() {
        assert!(matches!(
            shape("std::collections::BTreeMap<String, crate::datatypes::DataValue>").unwrap(),
            Shape::Map(item) if matches!(*item, Shape::Named(_))
        ));
    }

    #[test]
    fn an_option_is_the_union_with_null() {
        assert!(
            matches!(shape("Option<Vec<f64>>").unwrap(), Shape::Optional(item) if matches!(*item, Shape::Array(_)))
        );
    }

    #[test]
    fn ambiguous_spellings_are_refused_by_name() {
        assert!(refusal("HashMap<String, i32>").contains("order that changes"));
        assert!(refusal("u32").contains("Avro has no `u32`"));
        assert!(refusal("BTreeMap<i32, i32>").contains("keyed by `String`"));
        assert!(refusal("Option<Option<i32>>").contains("union"));
        assert!(refusal("[u8; 16]").contains("struct Name(pub [u8; N])"));
    }

    fn default(ty: &str, value: &str) -> String {
        let shape = shape(ty).unwrap();
        let expr = syn::parse_str::<Expr>(value).expect("an expression");
        default_value(&shape, &expr)
            .map(|t| t.to_string())
            .unwrap_or_else(|e| format!("error: {e}"))
    }

    #[test]
    fn a_default_is_written_the_way_the_schema_writes_it() {
        assert_eq!(
            default("String", r#""xml""#),
            r#":: std :: string :: String :: from ("xml")"#
        );
        let negative = default("i32", "-3").replace(' ', "");
        assert!(negative == "-3i32", "{negative}");
        assert_eq!(default("f64", "0"), "0f64");
        assert_eq!(
            default("Option<i32>", "null"),
            ":: core :: option :: Option :: None"
        );
        assert_eq!(default("Vec<String>", "[]"), ":: std :: vec ! []");
        assert_eq!(
            default("BTreeMap<String, i32>", "{}"),
            ":: std :: collections :: BTreeMap :: new ()"
        );
        assert_eq!(default("Vec<u8>", r#""\u{1}A""#), ":: std :: vec ! [1u8 , 65u8]");
        assert_eq!(default("IndexKind", "DateTime"), "< IndexKind > :: DateTime");
    }

    #[test]
    fn a_default_that_does_not_fit_its_field_is_refused() {
        assert!(default("i32", r#""3""#).starts_with("error: a default for an integer"));
        assert!(default("BTreeMap<String, i32>", "{ 1 }").starts_with("error"));
        assert!(default("i32", "3000000000").contains("out of range"));
        assert!(default("Vec<u8>", r#""é€""#).contains("code points"));
    }
}
