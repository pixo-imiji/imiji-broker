import { MongoConsumerService } from "./consumer/MongoConsumerService";
import { CustomTransportStrategy, Server } from "@nestjs/microservices";
import { ConfigService } from "@nestjs/config";
import { IEvent } from "imiji-api";

export class BrokerStrategy extends Server implements CustomTransportStrategy {
  constructor(
    private readonly consumer: MongoConsumerService,
    private readonly configService: ConfigService
  ) {
    super();
  }

  close() {
    console.log("Close BrokerStrategy");
  }

  async listen(callback: () => void) {
    const patterns = Array.from(this.getHandlers().keys());
    const channels = new Set(patterns.map((pattern) => pattern.split(">")[0]));
    if (channels.size > 0) {
      for (const channel of channels) {
        const events = patterns
          .filter((pattern) => pattern.split(channel + ">").length > 0)
          .map((pattern) => pattern.split(channel + ">")[1]);
        await this.consumer.consume({
          topic: channel,
          groupId: BrokerStrategy.name + "-" + channel,
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
