import { OnApplicationShutdown } from "@nestjs/common";

export interface IConsumerService extends OnApplicationShutdown {
  consume: (config: any) => Promise<any>;
}
