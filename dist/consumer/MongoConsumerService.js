"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MongoConsumerService = void 0;
const common_1 = require("@nestjs/common");
const mongoose_1 = require("mongoose");
const mongoose_transaction_decorator_1 = require("mongoose-transaction-decorator");
const api_1 = require("src/api");
const MongoConsumer_1 = require("./MongoConsumer");
const mongoose_2 = require("@nestjs/mongoose");
let MongoConsumerService = class MongoConsumerService {
    constructor(connection) {
        this.connection = connection;
        this.consumers = [];
    }
    async consume({ topic, groupId, user, onMessage }) {
        const consumer = new MongoConsumer_1.MongoConsumer(this.connection, topic, groupId, user);
        await consumer.connect();
        await consumer.consume(onMessage);
        this.consumers.push(consumer);
    }
    async onApplicationShutdown() {
        for (const consumer of this.consumers) {
            await consumer.disconnect();
        }
    }
};
__decorate([
    (0, mongoose_transaction_decorator_1.Transactional)(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], MongoConsumerService.prototype, "consume", null);
MongoConsumerService = __decorate([
    (0, common_1.Injectable)(),
    __param(0, (0, mongoose_2.InjectConnection)(api_1.BrokerConsumerDBConnectionName)),
    __metadata("design:paramtypes", [mongoose_1.Connection])
], MongoConsumerService);
exports.MongoConsumerService = MongoConsumerService;
