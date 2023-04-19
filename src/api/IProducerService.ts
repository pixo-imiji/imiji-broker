import { IEvent } from "imiji-server-api";

export interface IProducerService {
  produce: (topic: string, event: IEvent) => Promise<any>;
}
