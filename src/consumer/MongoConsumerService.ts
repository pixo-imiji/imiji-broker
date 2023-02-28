import { Injectable, OnApplicationShutdown } from "@nestjs/common";
import { Connection } from "mongoose";
import { Transactional } from "mongoose-transaction-decorator";
import { IConsumer } from "src/api";
import { MongoConsumer } from "./MongoConsumer";

@Injectable()
export class MongoConsumerService implements OnApplicationShutdown {
  private readonly consumers: IConsumer[] = [];

  constructor(private readonly connection: Connection) {}

  @Transactional()
  async consume({ topic, groupId, user, onMessage }) {
    const consumer = new MongoConsumer(this.connection, topic, groupId, user);
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
