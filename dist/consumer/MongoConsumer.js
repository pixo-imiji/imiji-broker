"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MongoConsumer = exports.dbName = void 0;
const common_1 = require("@nestjs/common");
const mongoose_transaction_decorator_1 = require("mongoose-transaction-decorator");
exports.dbName = "consumers";
class MongoConsumer {
    constructor(mongo, topic, groupId, user) {
        this.mongo = mongo;
        this.topic = topic;
        this.groupId = groupId;
        this.user = user;
        this.cachedConsumer = null;
        this.logger = new common_1.Logger(MongoConsumer.name);
        this.consumer = mongo.collection(exports.dbName);
    }
    async connect() {
        new mongoose_transaction_decorator_1.TransactionConnection().setConnection(this.mongo);
        this.logger.debug(`${this.groupId} consumer connected`);
    }
    async saveCommit(commitId) {
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
    async getLastConsumer() {
        return await this.consumer.findOne({
            topic: this.topic,
            groupId: this.groupId,
            user: this.user,
        });
    }
    async getLastCommitId() {
        const consumer = await this.getLastConsumer();
        if (consumer) {
            return consumer.commitId;
        }
        return null;
    }
    createStream() {
        const match = {
            $match: {
                operationType: "insert",
            },
        };
        const pipeline = [match];
        this.stream = this.mongo.collection(this.topic).watch(pipeline);
        return this.stream;
    }
    async consume(onEvent) {
        const { authInfo: { authenticatedUsers }, } = await this.mongo.db.command({ connectionStatus: 1 });
        if (authenticatedUsers.length === 0 ||
            authenticatedUsers[0].user !== this.user) {
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
        this.createStream().on("change", (next, _) => onEvent(next.fullDocument).then(() => this.saveCommit(next.fullDocument._id)));
    }
    async disconnect() {
        await this.stream.close();
        this.logger.debug(`consumer disconnected`);
    }
}
exports.MongoConsumer = MongoConsumer;
