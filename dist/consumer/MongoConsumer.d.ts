import { Connection } from "mongoose";
import { IConsumer } from "src/api";
export declare const dbName = "consumers";
export declare class MongoConsumer implements IConsumer {
    private readonly mongo;
    private readonly topic;
    private readonly groupId;
    private readonly user;
    private readonly logger;
    private readonly consumer;
    private stream;
    private cachedConsumer;
    constructor(mongo: Connection, topic: string, groupId: string, user: string);
    connect(): Promise<void>;
    private saveCommit;
    private getLastConsumer;
    private getLastCommitId;
    private createStream;
    consume(onEvent: (event: any) => Promise<void>): Promise<void>;
    disconnect(): Promise<void>;
}
