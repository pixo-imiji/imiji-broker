import { Logger } from "@nestjs/common";
import { Kafka, Consumer, SASLOptions } from "kafkajs";
import { IConsumer } from "../../api";

export class KafkaConsumer implements IConsumer {
  private readonly logger: Logger;
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;

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

  async consume(onEvent: (event) => Promise<void>) {
    await this.consumer.subscribe({ topic: this.topic });
    return this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ message, topic, partition, heartbeat }) => {
        const interval = setInterval(async () => await heartbeat(), 20 * 1000);
        try {
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
