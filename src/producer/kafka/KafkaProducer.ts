import { Logger } from "@nestjs/common";
import { Kafka, Producer } from "kafkajs";
import { IProducer } from "../../api";
import { IEvent } from "imiji-server-api";

export class KafkaProducer implements IProducer {
  private readonly logger: Logger;
  private readonly kafka: Kafka;
  private readonly producer: Producer;

  constructor(
    private readonly topic: string,
    private readonly brokers: string[],
    private readonly username: string,
    private readonly password: string,
    private readonly ssl: boolean
  ) {
    this.logger = new Logger(KafkaProducer.name);
    this.kafka = new Kafka({
      brokers: this.brokers,
      sasl: password
        ? {
            mechanism: "scram-sha-256",
            username,
            password,
          }
        : undefined,
      ssl,
    });
    this.producer = this.kafka.producer();
  }

  async connect() {
    await this.producer.connect();
  }

  async produce(message: IEvent): Promise<void> {
    await this.producer.send({
      topic: this.topic,
      messages: [{ value: JSON.stringify(message) }],
    });
  }

  async disconnect() {
    await this.producer.disconnect();
  }
}
