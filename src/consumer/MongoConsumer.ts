import { Logger } from "@nestjs/common";
import { ChangeStream } from "mongodb";
import { Collection, Connection } from "mongoose";
import {
  TransactionConnection,
  Transactional,
} from "mongoose-transaction-decorator";
import { IConsumer } from "src/api";

export const dbName = "consumers";

export class MongoConsumer implements IConsumer {
  private readonly logger: Logger;
  private readonly consumer: Collection;
  private stream;

  private cachedConsumer = null;

  constructor(
    private readonly mongo: Connection,
    private readonly topic: string,
    private readonly groupId: string,
    private readonly user: string
  ) {
    this.logger = new Logger(MongoConsumer.name);
    this.consumer = mongo.collection(dbName);
  }

  async connect() {
    new TransactionConnection().setConnection(this.mongo);
    this.logger.debug(`${this.groupId} consumer connected`);
  }

  private async saveCommit(commitId) {
    const consumer = await this.getLastConsumer();
    if (consumer || this.cachedConsumer) {
      const _id = consumer ? consumer._id : this.cachedConsumer._id;
      return this.consumer.updateOne({ _id }, { $set: { commitId } });
    }
    this.cachedConsumer = this.consumer.insertOne({
      topic: this.topic,
      groupId: this.groupId,
      user: this.user,
      commitId,
      createdAt: new Date().getTime(),
    });
  }

  private async getLastConsumer() {
    return await this.consumer.findOne({
      topic: this.topic,
      groupId: this.groupId,
      user: this.user,
    });
  }

  private async getLastCommitId() {
    const consumer = await this.getLastConsumer();
    if (consumer) {
      return consumer.commitId;
    }
    return null;
  }

  private createStream(): ChangeStream {
    const match = {
      $match: {
        operationType: "insert",
      },
    };
    const pipeline = [match];
    this.stream = this.mongo.collection(this.topic).watch(pipeline);
    return this.stream;
  }

  @Transactional()
  async consume(onEvent: (event) => Promise<void>) {
    const {
      authInfo: { authenticatedUsers },
    } = await this.mongo.db.command({ connectionStatus: 1 });
    if (
      authenticatedUsers.length === 0 ||
      authenticatedUsers[0].user !== this.user
    ) {
      throw new Error("No auth user provided");
    }
    const lastCommitId = await this.getLastCommitId();
    const query = lastCommitId ? { _id: { $gt: lastCommitId } } : {};
    const events = await this.mongo
      .collection(this.topic)
      .find(query)
      .sort({ timestamp: 1 })
      .toArray();
    for (let i = 0; i < events.length; i++) {
      const e = events[i];
      await onEvent(e);
      await this.saveCommit(e._id);
    }
    this.createStream().on("change", (next, _) =>
      onEvent(next.fullDocument).then(() =>
        this.saveCommit(next.fullDocument._id)
      )
    );
  }

  async disconnect() {
    await this.stream.close();
    this.logger.debug(`consumer disconnected`);
  }
}
