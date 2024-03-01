// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

// Allow unused code warnings because we share
// code between the two bin targets.
#![allow(dead_code)]
#![allow(unused_imports)]

mod standalone;

mod args;
mod auth_tokens;
mod cache;
mod emit;
mod errors;
mod file_fetcher;
mod http_util;
mod js;
mod node;
mod npm;
mod resolver;
mod util;
mod version;
mod worker;

use deno_core::error::generic_error;
use deno_core::error::AnyError;
use deno_core::error::JsError;
use deno_runtime::fmt_errors::format_js_error;
use deno_runtime::tokio_util::create_and_run_current_thread_with_maybe_metrics;
pub use deno_runtime::UNSTABLE_GRANULAR_FLAGS;
use deno_terminal::colors;

use std::env;
use std::env::current_exe;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::args::Flags;

pub(crate) fn unstable_exit_cb(feature: &str, api_name: &str) {
  eprintln!(
    "Unstable API '{api_name}'. The `--unstable-{}` flag must be provided.",
    feature
  );
  std::process::exit(70);
}

fn exit_with_message(message: &str, code: i32) -> ! {
  eprintln!(
    "{}: {}",
    colors::red_bold("error"),
    message.trim_start_matches("error: ")
  );
  std::process::exit(code);
}

fn unwrap_or_exit<T>(result: Result<T, AnyError>) -> T {
  match result {
    Ok(value) => value,
    Err(error) => {
      let mut error_string = format!("{error:?}");

      if let Some(e) = error.downcast_ref::<JsError>() {
        error_string = format_js_error(e);
      }

      exit_with_message(&error_string, 1);
    }
  }
}

fn main() {
  eprintln!(
    "{:?}: main",
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_millis()
  );
  let args: Vec<String> = env::args().collect();
  let future = async move {
    let current_exe_path = current_exe().unwrap();
    eprintln!(
      "{:?}: before extract_standalone",
      SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
    );
    match standalone::extract_standalone(&current_exe_path, args).await {
      Ok(Some((metadata, eszip))) => {
        eprintln!(
          "{:?}: after extract_standalone",
          SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
        );
        standalone::run(eszip, metadata).await
      }
      Ok(None) => Err(generic_error("No archive found.")),
      Err(err) => Err(err),
    }
  };

  unwrap_or_exit(create_and_run_current_thread_with_maybe_metrics(future));
}
