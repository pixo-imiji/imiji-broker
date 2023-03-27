import { Inject, Injectable, OnApplicationShutdown } from "@nestjs/common";
import { Connection } from "mongoose";
import { BrokerConsumerDBConnectionName, IConsumer } from "../api";
import { MongoConsumer } from "./MongoConsumer";
import { InjectConnection } from "@nestjs/mongoose";

@Injectable()
export class MongoConsumerService implements OnApplicationShutdown {
  private readonly consumers: IConsumer[] = [];

  constructor(
    @InjectConnection(BrokerConsumerDBConnectionName)
    private readonly connection: Connection,
    @Inject("REDIS") private readonly redisUrl: string
  ) {}

  async consume({ topic, groupId, user, onMessage }) {
    const consumer = new MongoConsumer(
      this.connection,
      this.redisUrl,
      topic,
      groupId,
      user
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
