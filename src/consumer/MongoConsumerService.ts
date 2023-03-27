import { Inject, Injectable, OnApplicationShutdown } from "@nestjs/common";
import { Connection } from "mongoose";
import { BrokerConsumerDBConnectionName, IConsumer } from "../api";
import { MongoConsumer } from "./MongoConsumer";
import { InjectConnection } from "@nestjs/mongoose";
import { ConfigService } from "@nestjs/config";

@Injectable()
export class MongoConsumerService implements OnApplicationShutdown {
  private readonly consumers: IConsumer[] = [];

  constructor(
    @InjectConnection(BrokerConsumerDBConnectionName)
    private readonly connection: Connection,
    private readonly configService: ConfigService
  ) {}

  async consume({ topic, groupId, user, onMessage }) {
    const consumer = new MongoConsumer(
      this.connection,
      this.configService.get("REDIS_DB"),
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
