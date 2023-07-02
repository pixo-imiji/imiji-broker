import { Logger } from "@nestjs/common";
import { ChangeStream } from "mongodb";
import { createClient, RedisClientType } from "redis";
import * as RedisLock from "redis-lock";
import { Collection, Connection } from "mongoose";
import { IConsumer } from "../../api";

export const dbName = "consumers";

export class MongoConsumer implements IConsumer {
  private readonly logger: Logger;
  private readonly consumer: Collection;
  private stream;
  private readonly redisClient: RedisClientType;
  private readonly redisLock;

  private cachedConsumer = null;

  constructor(
    private readonly mongo: Connection,
    private readonly redisUrl: string,
    private readonly topic: string,
    private readonly groupId: string,
    private readonly user: string
  ) {
    this.logger = new Logger(MongoConsumer.name);
    this.consumer = mongo.collection(dbName);
    this.redisClient = createClient({ url: redisUrl });
    this.redisLock = RedisLock(this.redisClient);
  }

  async connect() {
    await this.redisClient.connect();
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

  private async lock(event, onEvent: (event) => Promise<any>) {
    const lockKey = `lock:${event._id}:${this.groupId}`;
    const done = await this.redisLock(lockKey);
    await onEvent(event);
    await done();
  }

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
      const event = events[i];
      await this.lock(event, onEvent);
      await this.saveCommit(event._id);
    }
    this.logger.debug("start watch events");
    this.createStream().on(
      "change",
      async (next, _) =>
        await this.lock(next.fullDocument, (event) =>
          onEvent(event).then(() => this.saveCommit(next.fullDocument._id))
        )
    );
  }

  async disconnect() {
    await this.stream.close();
    await this.redisClient.quit();
    this.logger.debug(`consumer disconnected`);
  }
}
