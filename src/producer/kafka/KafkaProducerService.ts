import { IConsumerService } from "../../api";
import { Inject, Injectable } from "@nestjs/common";
import { KafkaProducer } from "./KafkaProducer";

@Injectable()
export class KafkaProducerService implements IConsumerService {
  private readonly consumers: KafkaProducer[] = [];

  constructor(
    @Inject("kafka-brokers")
    private readonly brokers: string[],
    @Inject("kafka-username")
    private readonly username: string,
    @Inject("kafka-password")
    private readonly password: string,
    @Inject("kafka-ssl")
    private readonly ssl: boolean
  ) {}

  async consume({ topic, groupId, onMessage }): Promise<any> {
    const consumer = new KafkaProducer(
      topic,
      groupId,
      this.brokers,
      this.username,
      this.password,
      this.ssl
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
