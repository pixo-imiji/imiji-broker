import { Injectable } from "@nestjs/common";
import { Connection } from "mongoose";
import {
  BrokerConsumerDBConnectionName,
  IConsumer,
  IConsumerService,
} from "../../api";
import { MongoConsumer } from "./MongoConsumer";
import { InjectConnection } from "@nestjs/mongoose";

@Injectable()
export class MongoConsumerService implements IConsumerService {
  private readonly consumers: IConsumer[] = [];

  constructor(
    @InjectConnection(BrokerConsumerDBConnectionName)
    private readonly connection: Connection
  ) {}

  async consume({ redisDB, topic, groupId, user, onMessage }) {
    const consumer = new MongoConsumer(
      this.connection,
      redisDB,
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