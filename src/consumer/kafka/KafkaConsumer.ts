import { Logger } from "@nestjs/common";
import { Consumer, Kafka, logLevel, SASLOptions } from "kafkajs";
import { IConsumer } from "../../api";
import { IEvent } from "imiji-server-api";

export class KafkaConsumer implements IConsumer {
  private readonly logger: Logger;
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;

  private readonly messageQueue = [];
  private timeout: number = 10000;
  private timer;

  constructor(
    private readonly topic: string,
    private readonly groupId: string,
    private readonly brokers: string[],
    private readonly username: string,
    private readonly password: string,
    private readonly ssl: boolean,
    private readonly mechanism: string
  ) {
    this.logger = new Logger(KafkaConsumer.name);
    this.kafka = new Kafka({
      brokers: this.brokers,
      logLevel: logLevel.ERROR,
      sasl: this.password
        ? this.createSasl(this.mechanism, this.username, this.password)
        : undefined,
      ssl,
    });
    this.consumer = this.kafka.consumer({ groupId: this.groupId });
  }

  private createSasl(
    mechanism: any,
    username: string,
    password: string
  ): SASLOptions {
    return {
      mechanism,
      username,
      password,
    };
  }

  async connect() {
    await this.consumer.connect();
  }

  private clearTimer() {
    if (!this.timer) clearTimeout(this.timer);
    this.timer = undefined;
  }

  private async processBatch(process: (message: IEvent) => Promise<void>) {
    this.clearTimer();
    const currentBatch = [];
    while (this.messageQueue.length > 0) {
      currentBatch.push(this.messageQueue.shift());
    }

    await Promise.allSettled(
      currentBatch.map(async (message) => await process(message.value))
    )
      .then(() =>
        currentBatch.map((message) => ({
          topic: message.topic,
          partition: message.partition,
          offset: (Number(message.offset) + 1).toString(),
        }))
      )
      .then((commits) => this.consumer.commitOffsets(commits));
  }

  private scheduleBatchProcessing(process: (message: IEvent) => Promise<void>) {
    this.clearTimer();
    this.timer = setTimeout(
      async () => this.processBatch(process),
      this.timeout
    );
  }

  async consume(
    onEvent: (event) => Promise<void>,
    isBatch?: boolean, //default false
    timeout?: number, // default 10 sec
    batchSize?: number // default 5
  ) {
    await this.consumer.subscribe({ topic: this.topic });
    return this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ message, topic, partition, heartbeat }) => {
        const interval = setInterval(async () => await heartbeat(), 20 * 1000);
        try {
          if (isBatch) {
            this.timeout = timeout ? timeout * 1000 : this.timeout;
            batchSize = batchSize ? batchSize : 5;
            this.messageQueue.push({
              value: JSON.parse(message.value.toString()),
              topic: partition,
              offset: message.offset,
            });
            if (this.messageQueue.length >= batchSize) {
              await this.processBatch(onEvent);
            } else {
              this.scheduleBatchProcessing(onEvent);
            }
          } else {
            await onEvent(JSON.parse(message.value.toString()))
              .then(() =>
                this.consumer.commitOffsets([
                  {
                    topic,
                    partition,
                    offset: (Number(message.offset) + 1).toString(),
                  },
                ])
              )
              .then(() => clearInterval(interval));
          }
        } catch (e) {
          clearInterval(interval);
          throw e;
        }
      },
    });
  }

  async disconnect() {
    await this.consumer.disconnect();
  }
}
