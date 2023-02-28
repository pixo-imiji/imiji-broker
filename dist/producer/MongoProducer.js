"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MongoProducer = void 0;
const common_1 = require("@nestjs/common");
const mongoose_transaction_decorator_1 = require("mongoose-transaction-decorator");
class MongoProducer {
    constructor(mongo, topic) {
        this.mongo = mongo;
        this.topic = topic;
        this.logger = new common_1.Logger(MongoProducer.name);
        this.collection = this.mongo.collection(topic);
    }
    async connect() {
        new mongoose_transaction_decorator_1.TransactionConnection().setConnection(this.mongo);
        this.logger.debug(`consumer connected`);
    }
    async disconnect() {
        this.logger.debug(`consumer disconnected`);
    }
    async produce(event) {
        const timestamp = new Date().getTime();
        await this.mongo.collection(this.topic).insertOne(Object.assign(Object.assign({}, event), { timestamp }));
    }
}
exports.MongoProducer = MongoProducer;
