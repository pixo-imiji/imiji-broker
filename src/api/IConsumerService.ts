export interface IConsumerService {
  consume: (config: any) => Promise<any>;
}
