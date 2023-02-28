import { Connection } from "mongoose";
import { IEvent } from "imiji-api";
import { IProducer } from "src/api";
export declare class MongoProducer implements IProducer {
    private readonly mongo;
    private readonly topic;
    private readonly logger;
    private readonly collection;
    constructor(mongo: Connection, topic: string);
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    produce(event: IEvent): Promise<void>;
}
