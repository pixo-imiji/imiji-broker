import { IProducerService } from "../../api";
import { Inject, Injectable } from "@nestjs/common";
import { KafkaProducer } from "./KafkaProducer";
import { IEvent } from "imiji-server-api";

@Injectable()
export class KafkaProducerService implements IProducerService {
  private readonly producers: Map<String, KafkaProducer> = new Map();

  constructor(
    @Inject("kafka-brokers")
    private readonly brokers: string[],
    @Inject("kafka-username")
    private readonly username: string,
    @Inject("kafka-password")
    private readonly password: string,
    @Inject("kafka-ssl")
    private readonly ssl: boolean,
    @Inject("kafka-mechanism")
    private readonly mechanism: string
  ) {}

  async produce(topic: string, event: IEvent): Promise<any> {
    let producer = this.producers.get(topic);
    if (!producer) {
      producer = new KafkaProducer(
        topic,
        this.brokers,
        this.username,
        this.password,
        this.mechanism,
        this.ssl
      );
      await producer.connect();
      this.producers.set(topic, producer);
    }
    await producer.produce(event);
  }

  async produceTx(
    topic: string,
    event: IEvent,
    run: () => Promise<any>
  ): Promise<any> {
    let producer = this.producers.get(topic);
    if (!producer) {
      producer = new KafkaProducer(
        topic,
        this.brokers,
        this.username,
        this.password,
        this.mechanism,
        this.ssl
      );
      await producer.connect();
      this.producers.set(topic, producer);
    }
    await producer.produceTx(run, event);
  }

  async onApplicationShutdown() {
    const producers = this.producers.values();
    for (const producer of producers) {
      await producer.disconnect();
    }
  }
}
