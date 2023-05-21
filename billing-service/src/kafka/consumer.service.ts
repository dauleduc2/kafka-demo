import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import {
  Consumer,
  ConsumerRunConfig,
  ConsumerSubscribeTopic,
  Kafka,
} from 'kafkajs';

@Injectable()
export class ConsumerService implements OnApplicationShutdown {
  private readonly kafka = new Kafka({
    clientId: 'demo',
    brokers: ['pkc-8jz80.ap-southeast-1.aws.confluent.cloud:9092'],
    ssl: true,
    sasl: {
      username: 'QOEZ4O4YBX26EAXP',
      password:
        'q/6IxrQLv2G2zOI/lxXN3EdGkA8FyoKfIA9V2cSgQIyJa+/BggRHlAMAnKgJfEDp',
      mechanism: 'plain',
    },
  });

  private readonly consumers: Consumer[] = [];

  async consume(topic: ConsumerSubscribeTopic, config: ConsumerRunConfig) {
    const consumer = this.kafka.consumer({
      groupId: 'test',
    });
    await consumer.connect();
    await consumer.subscribe(topic);
    await consumer.run(config);
    this.consumers.push(consumer);
  }

  async seek(
    topic: string,
    partition: number,
    offset: string,
    config: ConsumerRunConfig,
  ) {
    const consumer = this.kafka.consumer({
      groupId: 'test',
      allowAutoTopicCreation: true,
    });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    consumer.run({ autoCommit: false, ...config });
    consumer.seek({ topic, partition, offset });
  }

  async onApplicationShutdown() {
    for (const consumer of this.consumers) {
      await consumer.disconnect();
    }
  }
}
