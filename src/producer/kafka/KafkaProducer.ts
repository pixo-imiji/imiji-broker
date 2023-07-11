import { Logger } from "@nestjs/common";
import {
  Kafka,
  Partitioners,
  Producer,
  SASLMechanism,
  SASLMechanismOptions,
  SASLOptions,
} from "kafkajs";
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
    private readonly mechanism: string,
    private readonly ssl: boolean
  ) {
    this.logger = new Logger(KafkaProducer.name);
    this.kafka = new Kafka({
      brokers: this.brokers,
      sasl: this.password
        ? this.createSasl(this.mechanism, this.username, this.password)
        : undefined,
      ssl,
    });
    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
    });
  }

  private createSasl(
    mechanism: any,
    username: string,
    password: string
  ): SASLOptions {
    return {
      mechanism,
      username,
      password,
    };
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
