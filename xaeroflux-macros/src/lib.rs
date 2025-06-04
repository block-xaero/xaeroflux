use proc_macro::TokenStream;
use quote::quote;
use syn::{LitStr, parse_macro_input};

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
           use crate::{Subject, SubjectHash, XaeroEvent};
           use crate::system::control_bus::SystemPayload;
           use xaeroflux_core::event::{Event, EventType, SystemEventKind};

            // 1) Construct the Subject itself, calling new_with_workspace(...)
            let subject = Subject::new_with_workspace(
                #subject_name_tokens.to_string(),         // name: String
                [ #(#subject_bytes_tokens),* ],           // hash: [u8; 32]
                #ws_id_lit.to_string(),                    // workspace_id: String
                #obj_id_lit.to_string(),                   // object_id: String
            );

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
            subject.sink.tx.send(wc_evt)
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
            subject.sink.tx.send(oc_evt)
                .expect("failed to bootstrap: ObjectCreated");

            // 4) Return the newly‐constructed Arc<Subject>
            std::sync::Arc::new(subject)
        }
    };

    // 9) Convert into a TokenStream
    expanded.into()
}
