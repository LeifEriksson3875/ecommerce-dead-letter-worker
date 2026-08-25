use ecommerce_dead_letter_worker::{
    infrai_queue::InfraiQueue,
    order_worker::{record_failure, OrderJob},
};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let queue = InfraiQueue::from_env()?;
    queue.create("ecommerce-jobs-v1").await?;
    let batch = queue.consume(10, 30).await?;

    for message in batch.messages {
        let job: OrderJob = serde_json::from_value(message.payload)?;
        let decision = record_failure(
            &queue,
            &message.message_id,
            &job,
            "downstream job returned a domain rejection",
        ).await?;
        println!("order={} decision={decision:?}", job.order_id);
    }
    Ok(())
}

