import { Logger } from "@nestjs/common";
import { Kafka, Consumer } from "kafkajs";
import { IConsumer } from "../../api";

export class KafkaConsumer implements IConsumer {
  private readonly logger: Logger;
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;

  constructor(
    private readonly topic: string,
    private readonly groupId: string,
    private readonly brokers: string[],
    private readonly username: string,
    private readonly password: string,
    private readonly ssl: boolean
  ) {
    this.logger = new Logger(KafkaConsumer.name);
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
    this.consumer = this.kafka.consumer({ groupId: this.groupId });
  }

  async connect() {
    await this.consumer.connect();
  }

  async consume(onEvent: (event) => Promise<void>) {
    await this.consumer.subscribe({ topic: this.topic });
    return this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ message, topic, partition }) =>
        await onEvent(JSON.parse(message.value.toString())).then(() =>
          this.consumer.commitOffsets([
            {
              topic,
              partition,
              offset: message.offset,
            },
          ])
        ),
    });
  }

  async disconnect() {
    await this.consumer.disconnect();
  }
}