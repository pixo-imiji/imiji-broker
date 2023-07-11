import { IConsumerService } from "../../api";
import { Inject, Injectable } from "@nestjs/common";
import { KafkaConsumer } from "./KafkaConsumer";

@Injectable()
export class KafkaConsumerService implements IConsumerService {
  private readonly consumers: KafkaConsumer[] = [];

  constructor(
    @Inject("kafka-brokers")
    private readonly brokers: string[],
    @Inject("kafka-username")
    private readonly username: string,
    @Inject("kafka-password")
    private readonly password: string,
    @Inject("kafka-ssl")
    private readonly ssl: boolean,
    @Inject("kafka-mechanism")
    private readonly mechanism: string
  ) {}

  async consume({ topic, groupId, onMessage }): Promise<any> {
    const consumer = new KafkaConsumer(
      topic,
      groupId,
      this.brokers,
      this.username,
      this.password,
      this.ssl,
      this.mechanism
    );
    await consumer.connect();
    await consumer.consume(onMessage);
    this.consumers.push(consumer);
  }

  async onApplicationShutdown() {
    for (const consumer of this.consumers) {
      await consumer.disconnect();
    }
  }
}
