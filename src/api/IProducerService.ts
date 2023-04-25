import { IEvent } from "imiji-server-api";
import { OnApplicationShutdown } from "@nestjs/common";

export interface IProducerService extends OnApplicationShutdown {
  produce: (topic: string, event: IEvent) => Promise<any>;
}
