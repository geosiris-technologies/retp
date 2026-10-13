//! One function per kind of item: record, fixed, enumeration, union, message.

use heck::ToLowerCamelCase;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{Attribute, Data, DeriveInput, Expr, Fields, LitBool, LitInt, LitStr, Type};

use crate::shape::{self, Shape};

fn error<T>(span: impl Spanned, message: impl std::fmt::Display) -> syn::Result<T> {
    Err(syn::Error::new(span.span(), message))
}

/// The `#[avro(...)]` on the type itself.
#[derive(Default)]
struct TypeAttrs {
    name: Option<LitStr>,
    union: bool,
}

fn type_attrs(attrs: &[Attribute]) -> syn::Result<TypeAttrs> {
    let mut out = TypeAttrs::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("avro")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                out.name = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("union") {
                out.union = true;
            } else {
                return Err(meta.error("expected `name = \"...\"` or `union`"));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// The `#[avro(...)]` on a field.
#[derive(Default)]
struct FieldAttrs {
    rename: Option<LitStr>,
    default: Option<Expr>,
}

fn field_attrs(attrs: &[Attribute]) -> syn::Result<FieldAttrs> {
    let mut out = FieldAttrs::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("avro")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                out.rename = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("default") {
                out.default = Some(meta.value()?.parse()?);
            } else {
                return Err(meta.error("expected `rename = \"...\"` or `default = ...`"));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// The `#[avro(...)]` on a variant.
#[derive(Default)]
struct VariantAttrs {
    symbol: Option<LitStr>,
    default: bool,
}

fn variant_attrs(attrs: &[Attribute]) -> syn::Result<VariantAttrs> {
    let mut out = VariantAttrs::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("avro")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("symbol") {
                out.symbol = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("default") {
                out.default = true;
            } else {
                return Err(meta.error("expected `symbol = \"...\"` or `default`"));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

pub fn avro(input: &DeriveInput) -> syn::Result<TokenStream> {
    if !input.generics.params.is_empty() {
        return error(
            &input.generics,
            "an Avro type is concrete: the schema has no type parameters to give it",
        );
    }
    let attrs = type_attrs(&input.attrs)?;
    let Some(name) = attrs.name else {
        return error(
            &input.ident,
            "`#[avro(name = \"...\")]` is required: refusals name the type by its Avro full name",
        );
    };
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(_) if attrs.union => error(&input.ident, "`union` is for an enum"),
            Fields::Named(fields) => record(input, &name, fields),
            Fields::Unnamed(fields) => fixed(input, &name, fields),
            Fields::Unit => record(
                input,
                &name,
                &syn::FieldsNamed {
                    brace_token: Default::default(),
                    named: Default::default(),
                },
            ),
        },
        Data::Enum(data) if attrs.union => union(input, &name, data),
        Data::Enum(data) => enumeration(input, &name, data),
        Data::Union(_) => error(
            &input.ident,
            "a Rust `union` has no Avro encoding; an Avro union is an enum with `#[avro(union)]`",
        ),
    }
}

/// A record: every field in order, and a `Default` with the schema's values.
fn record(input: &DeriveInput, name: &LitStr, fields: &syn::FieldsNamed) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let mut writes = Vec::new();
    let mut reads = Vec::new();
    let mut defaults = Vec::new();
    let mut json_writes = Vec::new();
    let mut json_reads = Vec::new();
    let mut json_names = Vec::new();
    for field in &fields.named {
        let member = field.ident.as_ref().expect("a named field");
        let attrs = field_attrs(&field.attrs)?;
        let avro_name = match &attrs.rename {
            Some(rename) => rename.value(),
            None => member.unraw().to_string().to_lower_camel_case(),
        };
        let label = format!("{}.{avro_name}", name.value());
        let shape = shape::shape_of(&field.ty)?;
        writes.push(shape::write(&shape, quote!(&self.#member)));
        let read = shape::read(&shape, &label);
        reads.push(quote! { #member: #read? });
        let value = match &attrs.default {
            Some(expr) => shape::default_value(&shape, expr)?,
            None => quote!(::core::default::Default::default()),
        };
        defaults.push(quote! { #member: #value });
        // JSON: the schema's default stands in for an absent field; a field
        // without one is a refusal, not the Avro zero that `Default` gives.
        let json_write = shape::to_json(&shape, quote!(&self.#member));
        json_writes.push(quote! { entries.insert(::std::string::String::from(#avro_name), #json_write); });
        let json_read = shape::from_json(&shape, quote!(found), &label);
        let absent = match &attrs.default {
            Some(expr) => shape::default_value(&shape, expr)?,
            None => {
                quote! { return ::core::result::Result::Err(::retp_avro::json::missing(#name, #avro_name)) }
            }
        };
        json_reads.push(quote! {
            #member: match entries.get(#avro_name) {
                ::core::option::Option::Some(found) => #json_read?,
                ::core::option::Option::None => #absent,
            }
        });
        json_names.push(avro_name);
    }
    // A dozen ETP messages carry no field - `Ping`'s kin - and their codec
    // touches neither side, so the parameters take the underscore.
    let (out, input_param) = if fields.named.is_empty() {
        (format_ident!("_out"), format_ident!("_input"))
    } else {
        (format_ident!("out"), format_ident!("input"))
    };
    let json_self = if fields.named.is_empty() {
        format_ident!("_value")
    } else {
        format_ident!("value")
    };
    let json_mut = if fields.named.is_empty() {
        quote!()
    } else {
        quote!(mut)
    };
    Ok(quote! {
        impl ::retp_avro::Avro for #ident {
            const FULL_NAME: &'static str = #name;
            fn write(&self, #out: &mut ::retp_avro::Writer) {
                #(#writes)*
            }
            fn read(#input_param: &mut ::retp_avro::Reader<'_>) -> ::retp_avro::Result<Self> {
                ::core::result::Result::Ok(Self { #(#reads,)* })
            }
            fn to_json(&self) -> ::retp_avro::json::Value {
                let #json_mut entries = ::retp_avro::json::Map::new();
                #(#json_writes)*
                ::retp_avro::json::Value::Object(entries)
            }
            fn from_json(#json_self: &::retp_avro::json::Value) -> ::retp_avro::Result<Self> {
                let entries = ::retp_avro::json::as_object(#json_self, #name)?;
                ::retp_avro::json::check_fields(entries, #name, &[#(#json_names),*])?;
                ::core::result::Result::Ok(Self { #(#json_reads,)* })
            }
        }

        impl ::core::default::Default for #ident {
            fn default() -> Self {
                Self { #(#defaults,)* }
            }
        }
    })
}

/// A fixed: `struct Name(pub [u8; N])`, with no length on the wire.
fn fixed(input: &DeriveInput, name: &LitStr, fields: &syn::FieldsUnnamed) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let only = match fields.unnamed.iter().collect::<Vec<_>>().as_slice() {
        [only] => *only,
        _ => return error(fields, "an Avro fixed is `struct Name(pub [u8; N])`, one field"),
    };
    let Type::Array(array) = &only.ty else {
        return error(&only.ty, "an Avro fixed holds `[u8; N]`");
    };
    if !matches!(&*array.elem, Type::Path(p) if p.path.is_ident("u8")) {
        return error(&array.elem, "an Avro fixed holds bytes: `[u8; N]`");
    }
    Ok(quote! {
        impl ::retp_avro::Avro for #ident {
            const FULL_NAME: &'static str = #name;
            fn write(&self, out: &mut ::retp_avro::Writer) {
                out.fixed(&self.0);
            }
            fn read(input: &mut ::retp_avro::Reader<'_>) -> ::retp_avro::Result<Self> {
                ::core::result::Result::Ok(Self(input.fixed()?))
            }
            fn to_json(&self) -> ::retp_avro::json::Value {
                ::retp_avro::json::of_fixed(&self.0)
            }
            fn from_json(value: &::retp_avro::json::Value) -> ::retp_avro::Result<Self> {
                ::core::result::Result::Ok(Self(::retp_avro::json::as_fixed(value, #name)?))
            }
        }
    })
}

/// An enumeration: unit variants whose order is the ordinal on the wire.
fn enumeration(input: &DeriveInput, name: &LitStr, data: &syn::DataEnum) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let mut variants = Vec::new();
    let mut symbols = Vec::new();
    let mut default = None;
    for variant in &data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return error(
                variant,
                "an Avro enumeration has unit variants only; a union is marked `#[avro(union)]`",
            );
        }
        if let Some((_, discriminant)) = &variant.discriminant {
            return error(
                discriminant,
                "the variant order is the ordinal on the wire, so a variant takes no explicit discriminant",
            );
        }
        let attrs = variant_attrs(&variant.attrs)?;
        if attrs.default {
            if default.is_some() {
                return error(variant, "an enumeration has one default");
            }
            default = Some(variant.ident.clone());
        }
        symbols.push(match attrs.symbol {
            Some(symbol) => symbol.value(),
            None => variant.ident.unraw().to_string(),
        });
        variants.push(variant.ident.clone());
    }
    let Some(first) = variants.first() else {
        return error(ident, "an Avro enumeration has at least one symbol");
    };
    let default = default.unwrap_or_else(|| first.clone());
    let count = variants.len();
    let ordinals = 0..count;
    let ordinals_read = 0..count;
    Ok(quote! {
        impl #ident {
            /// Every symbol, in ordinal order.
            pub const SYMBOLS: [Self; #count] = [#(Self::#variants),*];
            /// The symbol's name, exactly as the schema spells it.
            pub fn as_str(&self) -> &'static str {
                match self { #(Self::#variants => #symbols,)* }
            }
        }

        impl ::core::default::Default for #ident {
            fn default() -> Self {
                Self::#default
            }
        }

        impl ::retp_avro::Avro for #ident {
            const FULL_NAME: &'static str = #name;
            fn write(&self, out: &mut ::retp_avro::Writer) {
                out.enum_index(match self { #(Self::#variants => #ordinals,)* });
            }
            fn read(input: &mut ::retp_avro::Reader<'_>) -> ::retp_avro::Result<Self> {
                ::core::result::Result::Ok(match input.enum_index(Self::FULL_NAME, #count)? {
                    #(#ordinals_read => Self::#variants,)*
                    _ => ::core::unreachable!("enum_index has already checked the range"),
                })
            }
            fn to_json(&self) -> ::retp_avro::json::Value {
                ::retp_avro::json::Value::String(::std::string::String::from(self.as_str()))
            }
            fn from_json(value: &::retp_avro::json::Value) -> ::retp_avro::Result<Self> {
                ::core::result::Result::Ok(match ::retp_avro::json::as_symbol(value, #name)? {
                    #(#symbols => Self::#variants,)*
                    other => return ::core::result::Result::Err(::retp_avro::json::unknown_symbol(#name, other)),
                })
            }
        }
    })
}

/// A union: a unit variant for the `null` branch, a one-field tuple variant
/// for every other, in branch order.
fn union(input: &DeriveInput, name: &LitStr, data: &syn::DataEnum) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let label = name.value();
    let mut writes = Vec::new();
    let mut reads = Vec::new();
    let mut nulls = 0;
    let mut json_writes = Vec::new();
    let mut json_null = None;
    let mut json_branches = Vec::new();
    for (index, variant) in data.variants.iter().enumerate() {
        let v = &variant.ident;
        match &variant.fields {
            Fields::Unit => {
                nulls += 1;
                if nulls > 1 {
                    return error(variant, "a union has one `null` branch at most");
                }
                writes.push(quote! { Self::#v => out.union_index(#index), });
                reads.push(quote! { #index => Self::#v, });
                json_writes.push(quote! { Self::#v => ::retp_avro::json::Value::Null, });
                json_null = Some(quote! { ::core::result::Result::Ok(Self::#v) });
            }
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let ty = &fields.unnamed[0].ty;
                let shape = shape::shape_of(ty)?;
                if matches!(shape, Shape::Optional(_) | Shape::Null) {
                    return error(
                        ty,
                        "a union branch cannot be a union; the `null` branch is a unit variant",
                    );
                }
                let write = shape::write(&shape, quote!(item));
                let read = shape::read(&shape, &label);
                writes.push(quote! { Self::#v(item) => { out.union_index(#index); #write } });
                reads.push(quote! { #index => Self::#v(#read?), });
                let branch = shape::branch_name(&shape);
                let json_write = shape::to_json(&shape, quote!(item));
                json_writes.push(quote! {
                    Self::#v(item) => ::retp_avro::json::tagged(#branch, #json_write),
                });
                let json_read = shape::from_json(&shape, quote!(inner), &label);
                json_branches.push(quote! {
                    if branch == #branch {
                        return ::core::result::Result::Ok(Self::#v(#json_read?));
                    }
                });
            }
            _ => {
                return error(
                    variant,
                    "a union branch is a unit variant (`null`) or holds exactly one value",
                );
            }
        }
    }
    let Some(first) = data.variants.first() else {
        return error(ident, "an Avro union has at least one branch");
    };
    // Avro applies a default to the first branch, always.
    let first_ident = &first.ident;
    let default = match first.fields {
        Fields::Unit => quote!(Self::#first_ident),
        _ => quote!(Self::#first_ident(::core::default::Default::default())),
    };
    let count = data.variants.len();
    // A union without a `null` branch cannot read the JSON `null`.
    let json_null = json_null.unwrap_or_else(|| {
        quote! { ::core::result::Result::Err(::retp_avro::json::unknown_branch(#name, "null")) }
    });
    Ok(quote! {
        impl ::core::default::Default for #ident {
            fn default() -> Self {
                #default
            }
        }

        impl ::retp_avro::Avro for #ident {
            const FULL_NAME: &'static str = #name;
            fn write(&self, out: &mut ::retp_avro::Writer) {
                match self { #(#writes)* }
            }
            fn read(input: &mut ::retp_avro::Reader<'_>) -> ::retp_avro::Result<Self> {
                ::core::result::Result::Ok(match input.union_index(Self::FULL_NAME, #count)? {
                    #(#reads)*
                    _ => ::core::unreachable!("union_index has already checked the range"),
                })
            }
            fn to_json(&self) -> ::retp_avro::json::Value {
                match self { #(#json_writes)* }
            }
            fn from_json(value: &::retp_avro::json::Value) -> ::retp_avro::Result<Self> {
                match ::retp_avro::json::union_branch(value, #name)? {
                    ::core::option::Option::None => #json_null,
                    ::core::option::Option::Some((branch, inner)) => {
                        #(#json_branches)*
                        let _ = inner;
                        ::core::result::Result::Err(::retp_avro::json::unknown_branch(#name, branch))
                    }
                }
            }
        }
    })
}

/// `#[etp(protocol = 4, message_type = 1, sender = "customer", roles = "store,customer", multipart)]`.
pub fn etp_message(input: &DeriveInput) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let mut protocol: Option<LitInt> = None;
    let mut message_type: Option<LitInt> = None;
    let mut sender: Option<LitStr> = None;
    let mut roles: Option<LitStr> = None;
    let mut multipart = false;
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("etp")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("protocol") {
                protocol = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("message_type") {
                message_type = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("sender") {
                sender = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("roles") {
                roles = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("multipart") {
                multipart = match meta.value() {
                    Ok(value) => value.parse::<LitBool>()?.value,
                    Err(_) => true,
                };
            } else {
                return Err(
                    meta.error("expected `protocol`, `message_type`, `sender`, `roles` or `multipart`")
                );
            }
            Ok(())
        })?;
    }
    let missing = |what: &str| syn::Error::new(ident.span(), format!("`#[etp({what} = ...)]` is required"));
    let protocol = protocol.ok_or_else(|| missing("protocol"))?;
    let message_type = message_type.ok_or_else(|| missing("message_type"))?;
    let sender = senders(&sender.ok_or_else(|| missing("sender"))?)?;
    let roles = senders(&roles.ok_or_else(|| missing("roles"))?)?;
    let protocol: i32 = protocol.base10_parse()?;
    let message_type: i32 = message_type.base10_parse()?;
    Ok(quote! {
        impl ::retp_avro::EtpMessage for #ident {
            const PROTOCOL: i32 = #protocol;
            const MESSAGE_TYPE: i32 = #message_type;
            const SENDER_ROLE: ::retp_avro::Senders = #sender;
            const PROTOCOL_ROLES: ::retp_avro::Senders = #roles;
            const MULTIPART: bool = #multipart;
        }
    })
}

/// `"store, customer"` becomes `Senders::These(&[Role::Store, Role::Customer])`.
///
/// `*` is its own variant: the two universal messages (§3.7.2) declare it, and
/// it is a different fact from a list of every role.
fn senders(declared: &LitStr) -> syn::Result<TokenStream> {
    let text = declared.value();
    if text.trim() == "*" {
        return Ok(quote!(::retp_avro::Senders::Any));
    }
    let mut roles = Vec::new();
    for role in text.split(',').map(str::trim).filter(|r| !r.is_empty()) {
        let variant = match role {
            "client" => "Client",
            "server" => "Server",
            "customer" => "Customer",
            "store" => "Store",
            "producer" => "Producer",
            "consumer" => "Consumer",
            other => return error(declared, format!("`{other}` is not one of the six ETP roles")),
        };
        let variant = format_ident!("{variant}");
        roles.push(quote!(::retp_avro::Role::#variant));
    }
    Ok(quote!(::retp_avro::Senders::These(&[#(#roles),*])))
}
