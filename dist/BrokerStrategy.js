"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BrokerStrategy = void 0;
const microservices_1 = require("@nestjs/microservices");
class BrokerStrategy extends microservices_1.Server {
    constructor(consumer, configService) {
        super();
        this.consumer = consumer;
        this.configService = configService;
    }
    async close() {
        return this.consumer.onApplicationShutdown();
    }
    async listen(callback) {
        const patterns = Array.from(this.getHandlers().keys());
        const channels = new Set(patterns.map((pattern) => pattern.split(">")[0]));
        if (channels.size > 0) {
            for (const channel of channels) {
                const arr = channel.split(":");
                const topic = arr[0];
                const groupId = arr.length === 2
                    ? BrokerStrategy.name + "-" + topic + "-" + arr[1]
                    : BrokerStrategy.name + "-" + topic;
                const events = patterns
                    .filter((pattern) => pattern.split(channel + ">").length > 0)
                    .map((pattern) => pattern.split(channel + ">")[1]);
                await this.consumer.consume({
                    topic,
                    groupId: groupId,
                    user: this.configService.get("BROKER_USER"),
                    onMessage: async (event) => {
                        const handlerNames = events.filter((eventName) => eventName.split(":").length > 0
                            ? eventName.split(":")[0] === event.eventName
                            : eventName === event.eventName);
                        for (const pattern of handlerNames) {
                            const handler = this.getHandlers().get(channel + ">" + pattern);
                            await handler(event);
                        }
                    },
                });
            }
        }
        callback();
    }
}
exports.BrokerStrategy = BrokerStrategy;
