use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use actix_web::http::StatusCode;
use actix_web::web::Data;
use actix_web::{FromRequest, HttpRequest, HttpResponse, ResponseError};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{CommitMode, Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::OwnedHeaders;
use rdkafka::metadata::Metadata;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

#[derive(Clone)]
pub struct Kafka {
    pub(crate) admin: Arc<AdminClient<DefaultClientContext>>,
    pub(crate) consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
    pub(crate) metadata: HashMap<String, Arc<Metadata>>,
    pub(crate) producer: FutureProducer,
}

impl Kafka {
    pub fn new<S: ToString, T: ToString + Clone>(server: S, topics: Vec<T>) -> Self {
        let server = server.to_string();
        let mut client = ClientConfig::new()
            .set("bootstrap.servers", &server)
            .set("message.timeout.ms", "1000")
            .to_owned();

        let admin = client
            .create::<AdminClient<DefaultClientContext>>()
            .expect("AdminClient creation error");

        let producer = client
            .create::<FutureProducer>()
            .expect("Producer creation error");

        client.remove("message.timeout.ms");
        client.set("group.id", "group");
        client.set("enable.partition.eof", "false");
        client.set("session.timeout.ms", "6000");
        client.set("enable.auto.commit", "true");
        client.set("enable.auto.offset.store", "false");

        let consumer = client
            .create::<StreamConsumer<DefaultConsumerContext>>()
            .expect("Consumer creation error");

        let topics = topics.iter().map(|t| t.to_string()).collect::<Vec<_>>();

        let mut metadata = HashMap::new();

        for topic in topics.iter() {
            let topic = topic.to_string();
            let arc = Arc::new(
                consumer
                    .fetch_metadata(Some(&topic), Duration::from_secs(1))
                    .expect("Failed to fetch metadata"),
            );
            metadata.insert(topic, arc);
        }

        consumer
            .subscribe(
                topics
                    .iter()
                    .map(|t| t.as_str())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
            .expect("Failed to subscribe to topic");

        let admin = Arc::new(admin);
        let consumer = Arc::new(consumer);

        Self {
            admin,
            consumer,
            metadata,
            producer,
        }
    }

    pub async fn latest<T: ToString>(&self, topic: T) -> Result<Vec<u8>, KafkaError> {
        let topic = topic.to_string();
        let consumer = self.consumer.clone();
        let mut tpl = TopicPartitionList::new();
        let metadata = self.metadata.get(&topic).unwrap();

        for metadata in metadata.topics().iter() {
            for partition in metadata.partitions() {
                tpl.add_partition_offset(&topic, partition.id(), Offset::OffsetTail(1))?;
            }
        }

        consumer.assign(&tpl)?;

        loop {
            let message = consumer.recv().await?;

            consumer.commit_message(&message, CommitMode::Async)?;

            if topic.eq(message.topic()) {
                return Ok(message.payload().unwrap().to_vec());
            }
        }
    }

    pub fn consume<T: ToString, F, R>(&self, topic: T, callback: F) -> Result<(), KafkaError>
    where
        F: Fn(Result<Vec<u8>, KafkaError>) -> R + Clone + Send + 'static,
        R: Future + Send + 'static,
    {
        let topic = topic.to_string();
        let consumer = self.consumer.clone();
        let mut tpl = TopicPartitionList::new();
        let metadata = self.metadata.get(&topic).unwrap();

        for metadata in metadata.topics().iter() {
            for partition in metadata.partitions() {
                tpl.add_partition_offset(&topic, partition.id(), Offset::End)?;
            }
        }

        consumer.assign(&tpl)?;

        tokio::spawn(async move {
            loop {
                match consumer.recv().await {
                    Ok(message) => {
                        consumer
                            .commit_message(&message, CommitMode::Async)
                            .unwrap();

                        if topic.eq(message.topic()) {
                            callback(Ok(message.payload().unwrap().to_vec())).await;
                        }
                    }
                    Err(e) => {
                        callback(Err(e)).await;
                    }
                }
            }
        });

        Ok(())
    }

    pub fn push<T: ToString, P: AsRef<[u8]>>(&self, topic: T, payload: P) {
        let topic = topic.to_string();
        let payload = payload.as_ref().to_vec();
        let message = OwnedHeaders::new();
        let producer = self.producer.clone();

        tokio::spawn(async move {
            let record = FutureRecord::to(&topic)
                .key("key")
                .payload(&payload)
                .headers(message);

            match producer.send(record, Duration::from_secs(0)).await {
                Ok(_) => println!("Pushed to topic {}", topic),
                Err((e, _)) => eprintln!("Error pushing to topic {} {}", topic, e),
            }
        });
    }

    pub fn push_delay<T: ToString, P: AsRef<[u8]>>(
        &self,
        topic: T,
        payload: P,
        duration: Duration,
    ) {
        let pusher = self.clone();
        let topic = topic.to_string();
        let payload = payload.as_ref().to_vec();

        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            pusher.push(topic, payload);
        });
    }

    pub async fn create_topic<T: ToString>(&self, name: T) {
        let name = name.to_string();
        let topic = NewTopic::new(name.as_str(), 1, TopicReplication::Fixed(0));
        let result = self
            .admin
            .create_topics(&[topic], &AdminOptions::new())
            .await;

        match result {
            Ok(_) => println!("Topic {} created", name),
            Err(e) => eprintln!("Error creating topic {} {}", name, e),
        }
    }

    pub async fn delete_topic<T: ToString>(&self, name: T) {
        let name = name.to_string();
        let result = self
            .admin
            .delete_topics(&[&name], &AdminOptions::new())
            .await;

        match result {
            Ok(_) => println!("Topic {} deleted", name),
            Err(e) => eprintln!("Error deleting topic {} {}", name, e),
        }
    }
}

unsafe impl Send for Kafka {}

#[derive(Clone, Copy, Debug)]
pub enum KafkaBindingError {
    NotConfigured,
}

impl fmt::Display for KafkaBindingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use KafkaBindingError::*;

        match self {
            NotConfigured => write!(f, "Kafka not configured"),
        }
    }
}

impl ResponseError for KafkaBindingError {
    fn status_code(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }

    fn error_response(&self) -> actix_web::HttpResponse {
        HttpResponse::InternalServerError().finish()
    }
}

impl FromRequest for Kafka {
    type Error = KafkaBindingError;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut actix_web::dev::Payload) -> Self::Future {
        let kafka = req.app_data::<Data<Kafka>>().cloned();

        Box::pin(async move {
            match kafka {
                Some(kafka) => {
                    let kafka = Kafka {
                        admin: kafka.admin.clone(),
                        consumer: kafka.consumer.clone(),
                        metadata: kafka.metadata.clone(),
                        producer: kafka.producer.clone(),
                    };

                    Ok(kafka)
                }
                None => Err(KafkaBindingError::NotConfigured),
            }
        })
    }
}
