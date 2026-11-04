//! Build metadata supplied by the consuming build environment.

pub const CI_COMMIT_SHORT_SHA: Option<&str> = option_env!("CI_COMMIT_SHORT_SHA");
