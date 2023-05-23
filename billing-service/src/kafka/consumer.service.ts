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
    brokers: ['pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092'],
    ssl: true,
    sasl: {
      username: 'GQPQ3OIAKR2UVZH4',
      password:
        'g/TeVSTzGq3s7GHIYmDlQUUOsw2dgPfVy1APv0QQGquf5DvlSY1naiB+8o34t29s',
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
    consumer.run({ ...config });
    consumer.seek({ topic, partition, offset });
  }

  async onApplicationShutdown() {
    for (const consumer of this.consumers) {
      await consumer.disconnect();
    }
  }
}
