import { OnApplicationShutdown } from "@nestjs/common";
import { Connection } from "mongoose";
export declare class MongoConsumerService implements OnApplicationShutdown {
    private readonly connection;
    private readonly consumers;
    constructor(connection: Connection);
    consume({ topic, groupId, user, onMessage }: {
        topic: any;
        groupId: any;
        user: any;
        onMessage: any;
    }): Promise<void>;
    onApplicationShutdown(): Promise<void>;
}
