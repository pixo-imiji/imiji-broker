import { Connection } from "mongoose";
import { OnApplicationShutdown } from "@nestjs/common";
import { IEvent } from "imiji-api";
export declare class MongoProducerService implements OnApplicationShutdown {
    private readonly connection;
    private readonly producers;
    constructor(connection: Connection);
    produce(topic: string, event: IEvent): Promise<void>;
    private getProducerOfTopic;
    onApplicationShutdown(): Promise<void>;
}
