import { Logger } from "@nestjs/common";
import { Kafka, logLevel, Partitioners, Producer, SASLOptions } from "kafkajs";
import { v4 as uuidv4 } from "uuid";

import { IEvent } from "imiji-server-api";

import { IProducer } from "../../api";

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
      logLevel: logLevel.ERROR,
      sasl: this.password
        ? this.createSasl(this.mechanism, this.username, this.password)
        : undefined,
      ssl,
    });
    this.producer = this.kafka.producer({
      transactionalId: "transactional-" + this.topic + "-" + uuidv4(),
      maxInFlightRequests: 1,
      idempotent: true,
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

  async produceTx(run: () => Promise<any>, event: IEvent): Promise<any> {
    const transaction = await this.producer.transaction();
    try {
      await transaction.send({
        topic: this.topic,
        messages: [{ value: JSON.stringify(event) }],
      });
      await run();
      await transaction.commit();
    } catch (e) {
      await transaction.abort();
    }
  }

  async disconnect() {
    await this.producer.disconnect();
  }
}
