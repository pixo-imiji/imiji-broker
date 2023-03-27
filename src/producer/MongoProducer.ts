import { Logger } from "@nestjs/common";
import { Connection } from "mongoose";
import {
  Transactional,
  TransactionConnection,
} from "mongoose-transaction-decorator";
import { IEvent } from "imiji-server-api";
import { IProducer } from "src/api";

export class MongoProducer implements IProducer {
  private readonly logger: Logger;
  private readonly collection;

  constructor(
    private readonly mongo: Connection,
    private readonly topic: string
  ) {
    this.logger = new Logger(MongoProducer.name);
    this.collection = this.mongo.collection(topic);
  }

  async connect() {
    new TransactionConnection().setConnection(this.mongo);
    this.logger.debug(`consumer connected`);
  }

  async disconnect() {
    this.logger.debug(`consumer disconnected`);
  }

  @Transactional()
  async produce(event: IEvent) {
    const timestamp = new Date().getTime();
    await this.mongo.collection(this.topic).insertOne({ ...event, timestamp });
  }
}
