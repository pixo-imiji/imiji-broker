import { Connection } from "mongoose";
import { Injectable } from "@nestjs/common";
import { IEvent } from "imiji-server-api";
import { MongoProducer } from "./MongoProducer";
import {
  BrokerProducerDBConnectionName,
  IProducer,
  IProducerService,
} from "../api";
import { InjectConnection } from "@nestjs/mongoose";

@Injectable()
export class MongoProducerService implements IProducerService {
  private readonly producers = new Map<string, IProducer>();

  constructor(
    @InjectConnection(BrokerProducerDBConnectionName)
    private readonly connection: Connection
  ) {}

  async produce(topic: string, event: IEvent) {
    await (await this.getProducerOfTopic(topic)).produce(event);
  }

  private async getProducerOfTopic(topic: string) {
    let producer = this.producers.get(topic);
    if (!producer) {
      producer = new MongoProducer(this.connection, topic);
      this.producers.set(topic, producer);
    }
    return producer;
  }

  async onApplicationShutdown() {
    for (const producer of this.producers.values()) {
      await producer.disconnect();
    }
  }
}
