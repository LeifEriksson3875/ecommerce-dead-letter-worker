use crate::infrai_queue::{InfraiQueue, QueueError};
use serde::{Deserialize, Serialize};
use serde_json::to_value;
use thiserror::Error;

pub const MAX_ATTEMPTS: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStage {
    Checkout,
    Fulfillment,
    Receipt,
    CustomerOrderUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderJob {
    pub order_id: String,
    pub stage: OrderStage,
    pub attempt: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub order_id: String,
    pub failed_stage: OrderStage,
    pub attempts: u8,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureDecision {
    Retry(OrderJob),
    DeadLetter(DeadLetter),
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Queue(#[from] QueueError),
    #[error("message payload is invalid: {0}")]
    Payload(#[from] serde_json::Error),
}

pub fn classify_failure(job: &OrderJob, reason: impl Into<String>) -> FailureDecision {
    let attempts = job.attempt.saturating_add(1);
    if attempts >= MAX_ATTEMPTS {
        FailureDecision::DeadLetter(DeadLetter {
            order_id: job.order_id.clone(),
            failed_stage: job.stage,
            attempts,
            reason: reason.into(),
        })
    } else {
        FailureDecision::Retry(OrderJob { attempt: attempts, ..job.clone() })
    }
}

pub async fn record_failure(
    queue: &InfraiQueue,
    message_id: &str,
    job: &OrderJob,
    reason: &str,
) -> Result<FailureDecision, WorkerError> {
    let decision = classify_failure(job, reason);
    let (payload, publish_key) = match &decision {
        FailureDecision::Retry(next) => (
            to_value(next)?,
            format!("order:{}:{:?}:attempt:{}", next.order_id, next.stage, next.attempt),
        ),
        FailureDecision::DeadLetter(dead) => (
            to_value(dead)?,
            format!("dead-letter:{}:{:?}", dead.order_id, dead.failed_stage),
        ),
    };
    queue.publish(payload, &publish_key).await?;
    queue.ack(message_id, &format!("ack:{message_id}")).await?;
    Ok(decision)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn third_receipt_failure_becomes_a_dead_letter() {
        let job = OrderJob {
            order_id: "ord-1042".into(),
            stage: OrderStage::Receipt,
            attempt: 2,
        };

        assert_eq!(
            classify_failure(&job, "receipt provider rejected the address"),
            FailureDecision::DeadLetter(DeadLetter {
                order_id: "ord-1042".into(),
                failed_stage: OrderStage::Receipt,
                attempts: 3,
                reason: "receipt provider rejected the address".into(),
            })
        );
    }
}

