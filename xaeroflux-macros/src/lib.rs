use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use syn::{DeriveInput, LitStr, parse_macro_input};

#[proc_macro_derive(PipeKind, attributes(pipe_kind))]
pub fn derive_pipe_kind(input: TokenStream) -> TokenStream {
    // Parse the struct we're deriving for.
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    // Look for exactly one #[pipe_kind(...)] attribute.
    let mut found: Option<Ident> = None;
    for attr in input.attrs.into_iter() {
        if attr.path().is_ident("pipe_kind") {
            if found.is_some() {
                panic!("Multiple `#[pipe_kind]` attributes found. Only one is allowed.");
            }
            // Parse the contents as a single Ident (e.g. Control or Data).
            let kind_ident = attr.parse_args::<Ident>().expect(
                "`#[pipe_kind(...)]` must contain exactly one bare identifier, e.g. `Control` or \
                 `Data`",
            );
            found = Some(kind_ident);
        }
    }

    let kind_ident =
        found.expect("Missing `#[pipe_kind(Control)]` or `#[pipe_kind(Data)]` attribute.");

    // Map that identifier into the corresponding BusKind variant.
    let bus_variant = match kind_ident.to_string().as_str() {
        "Control" => quote! { BusKind::Control },
        "Data" => quote! { BusKind::Data },
        other => panic!(
            "`#[pipe_kind(...)]` must be either `Control` or `Data`, got `{}`",
            other
        ),
    };

    // Generate `impl StructName { pub fn new(bounds: Option<usize>) -> Self {
    // StructName(Pipe::new(BusKind::X, bounds)) } }`.
    let expanded = quote! {
        impl #struct_name {
            pub fn new(bounds: Option<usize>) -> Self {
                use crossbeam::channel::Sender;
                use crossbeam::channel::Receiver;
                use crossbeam::channel::bounded;
                let (source_tx, source_rx) =  crossbeam::channel::bounded(bounds.unwrap_or(100));
                let (sink_tx, sink_rx) =  crossbeam::channel::bounded(bounds.unwrap_or(100));
                let source = NetworkSource{rx: source_rx, tx: source_tx};
                let sink = NetworkSink{tx: sink_tx, rx: sink_rx};
                #struct_name(Arc::new(crate::networking::p2p::NetworkPipe{source,
                    sink, bus_kind: #bus_variant,
                    bounds}))
            }
        }
    };
    TokenStream::from(expanded)
}
#[proc_macro]
pub fn subject(input: TokenStream) -> TokenStream {
    // 1) Parse exactly one string literal: subject!("workspace/MyWS/object/MyObj")
    let subject_name_tokens = parse_macro_input!(input as LitStr);
    let span = subject_name_tokens.span();
    let literal_str = subject_name_tokens.value();

    // 2) Split on ‘/’ and validate that we get exactly 4 parts:
    let parts: Vec<_> = literal_str.split('/').collect();
    if parts.len() != 4 {
        return syn::Error::new_spanned(
            &subject_name_tokens,
            "Subject must look like \"workspace/<workspace_id>/object/<object_id>\"",
        )
        .to_compile_error()
        .into();
    }

    // 3) Check the “workspace” / “object” prefixes:
    if parts[0] != "workspace" || parts[2] != "object" {
        return syn::Error::new_spanned(
            &subject_name_tokens,
            "Subject must look like \"workspace/<workspace_id>/object/<object_id>\"",
        )
        .to_compile_error()
        .into();
    }

    // 4) Extract the actual IDs:
    let ws_id_str = parts[1]; // e.g. "MyWorkspace"
    let obj_id_str = parts[3]; // e.g. "MyObject"

    if ws_id_str.is_empty() || obj_id_str.is_empty() {
        return syn::Error::new_spanned(
            &subject_name_tokens,
            "workspace_id and object_id cannot be empty",
        )
        .to_compile_error()
        .into();
    }

    // 5) Compute three separate blake3 hashes:
    //
    //    a. hash of workspace_id
    //    b. hash of object_id
    //    c. hash of (workspace_hash || object_hash)
    //
    let mut h = blake3::Hasher::new();
    h.update(ws_id_str.as_bytes());
    let ws_hash = h.finalize();
    let ws_bytes = ws_hash.as_bytes();

    let mut h = blake3::Hasher::new();
    h.update(obj_id_str.as_bytes());
    let obj_hash = h.finalize();
    let obj_bytes = obj_hash.as_bytes();

    let mut h = blake3::Hasher::new();
    h.update(ws_bytes);
    h.update(obj_bytes);
    let subject_hash = h.finalize();
    let subject_bytes = subject_hash.as_bytes();

    // 6) Turn each 32‐byte array into a Vec<TokenStream> of byte‐literals:
    let subject_bytes_tokens = subject_bytes
        .iter()
        .map(|b| quote! { #b })
        .collect::<Vec<_>>();

    let ws_bytes_tokens = ws_bytes.iter().map(|b| quote! { #b }).collect::<Vec<_>>();

    let obj_bytes_tokens = obj_bytes.iter().map(|b| quote! { #b }).collect::<Vec<_>>();

    // 7) Build LitStrs for workspace_id and object_id (reusing the same span):
    let ws_id_lit = LitStr::new(ws_id_str, span);
    let obj_id_lit = LitStr::new(obj_id_str, span);

    // 8) Assemble the final token‐stream.
    let expanded = quote! {
        {
            // Bring everything into scope from the current crate:
            use crate::subject::SubjectHash;
            use crate::XaeroEvent;
            use crate::subject::Subject;
            use xaeroflux_core::event::{Event, EventType, SystemEventKind};
            use crate::{BusKind, Pipe};
            // 1) Construct the Subject itself, calling new_with_workspace(...)
            let subject = Subject::new_with_workspace(
                #subject_name_tokens.to_string(),         // name: String
                [ #(#subject_bytes_tokens),* ],           // hash: [u8; 32]
            #ws_id_lit.to_string(),                    // workspace_id: String
            #obj_id_lit.to_string(),                   // object_id: String
            );
            let control_pipe = Pipe::new(BusKind::Control,Some(100));
            let data_pipe = Pipe::new(BusKind::Data,Some(100));
            // 2) Emit a “WorkspaceCreated” system event
            let wc_evt = XaeroEvent {
                evt: xaeroflux_core::event::Event::new(
                    vec![ #(#ws_bytes_tokens),* ],       // payload = workspace_id bytes
                    EventType::SystemEvent(
                        SystemEventKind::WorkspaceCreated
                    ).to_u8()
                ),
                merkle_proof: None,
            };
            subject.control.sink.tx.send(wc_evt)
                .expect("failed to bootstrap: WorkspaceCreated");

            // 3) Emit an “ObjectCreated” system event
            let oc_evt = XaeroEvent {
                evt: xaeroflux_core::event::Event::new(
                    vec![ #(#obj_bytes_tokens),* ],      // payload = object_id bytes
                    EventType::SystemEvent(
                        SystemEventKind::ObjectCreated
                    ).to_u8()
                ),
                merkle_proof: None,
            };
            subject.control.sink.tx.send(oc_evt)
                .expect("failed to bootstrap: ObjectCreated");

            // 4) Return the newly‐constructed Arc<Subject>
            std::sync::Arc::new(subject)
        }
    };

    // 9) Convert into a TokenStream
    expanded.into()
}
