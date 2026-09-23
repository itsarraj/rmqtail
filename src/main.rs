use anyhow::{Context, Result};
use clap::Parser;
use futures_lite::stream::StreamExt;
use lapin::{
    options::{BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// RabbitMQ URL (e.g., amqp://guest:guest@127.0.0.1:5672/%2f)
    #[arg(default_value = "amqp://127.0.0.1:5672/%2f")]
    url: String,

    /// Exchange to bind to (creates a temporary queue bound to it)
    #[arg(short, long)]
    exchange: Option<String>,

    /// Queue to tail (if exchange is not provided)
    #[arg(short, long)]
    queue: Option<String>,

    /// Routing key to use when binding to an exchange
    #[arg(short, long, default_value = "#")]
    routing_key: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.exchange.is_none() && args.queue.is_none() {
        anyhow::bail!("Must provide either --exchange or --queue");
    }

    let conn = Connection::connect(&args.url, ConnectionProperties::default())
        .await
        .context("Failed to connect to RabbitMQ")?;

    let channel = conn
        .create_channel()
        .await
        .context("Failed to create channel")?;

    let queue_name = if let Some(ref ex) = args.exchange {
        // Create an exclusive, auto-delete queue
        let opts = QueueDeclareOptions {
            exclusive: true,
            auto_delete: true,
            ..Default::default()
        };
        let queue = channel
            .queue_declare("", opts, FieldTable::default())
            .await
            .context("Failed to declare temporary queue")?;

        let q_name = queue.name().as_str();

        channel
            .queue_bind(
                q_name,
                ex,
                &args.routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("Failed to bind temporary queue to exchange")?;

        q_name.to_string()
    } else {
        args.queue.unwrap()
    };

    println!("Tailing messages...");

    let opts = BasicConsumeOptions {
        no_ack: true,
        ..Default::default()
    };

    let mut consumer = channel
        .basic_consume(&queue_name, "rmqtail", opts, FieldTable::default())
        .await
        .context("Failed to start consuming")?;

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.context("Error in consumer stream")?;
        let body = String::from_utf8_lossy(&delivery.data);
        println!("{}", body);
    }

    Ok(())
}
