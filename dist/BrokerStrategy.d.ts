import { MongoConsumerService } from "./consumer/MongoConsumerService";
import { CustomTransportStrategy, Server } from "@nestjs/microservices";
import { ConfigService } from "@nestjs/config";
export declare class BrokerStrategy extends Server implements CustomTransportStrategy {
    private readonly consumer;
    private readonly configService;
    constructor(consumer: MongoConsumerService, configService: ConfigService);
    close(): void;
    listen(callback: () => void): Promise<void>;
}
