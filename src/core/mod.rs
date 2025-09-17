pub mod contextual_pipeline;
pub mod etl;
pub mod mvp_pipeline;
pub mod pipeline;
pub mod pipeline_sequence;

pub use crate::domain::model::{Record, TransformResult};
pub use crate::domain::ports::{ConfigProvider, Pipeline, Storage};
pub use crate::utils::error::Result;
