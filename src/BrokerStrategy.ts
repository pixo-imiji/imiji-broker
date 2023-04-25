import { CustomTransportStrategy, Server } from "@nestjs/microservices";
import { ConfigService } from "@nestjs/config";
import { IEvent } from "imiji-server-api";
import { Inject } from "@nestjs/common";
import { IConsumerService } from "./api";

export class BrokerStrategy extends Server implements CustomTransportStrategy {
  constructor(
    @Inject("IConsumerService") private readonly consumer: IConsumerService,
    private readonly configService: ConfigService
  ) {
    super();
  }

  async close() {
    return this.consumer.onApplicationShutdown();
  }

  async listen(callback: () => void) {
    const patterns = Array.from(this.getHandlers().keys());
    const channels = new Set(patterns.map((pattern) => pattern.split(">")[0]));
    if (channels.size > 0) {
      for (const channel of channels) {
        const arr = channel.split(":");
        const topic = arr[0];
        const groupId =
          arr.length === 2
            ? BrokerStrategy.name + "-" + topic + "-" + arr[1]
            : BrokerStrategy.name + "-" + topic;
        const events = patterns
          .filter((pattern) => pattern.split(channel + ">").length > 0)
          .map((pattern) => pattern.split(channel + ">")[1])
          .filter((pattern) => !!pattern);
        await this.consumer.consume({
          topic,
          redisDB: this.configService.get("REDIS_DB"),
          groupId: groupId,
          user: this.configService.get("BROKER_USER"),
          onMessage: async (event: IEvent) => {
            const handlerNames = events.filter((eventName) =>
              eventName.split(":").length > 0
                ? eventName.split(":")[0] === event.eventName
                : eventName === event.eventName
            );
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
