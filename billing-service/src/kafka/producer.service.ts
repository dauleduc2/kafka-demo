import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { Kafka, Producer, ProducerRecord } from 'kafkajs';

@Injectable()
export class ProducerService implements OnModuleInit, OnApplicationShutdown {
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

  private readonly producer: Producer = this.kafka.producer();

  async onModuleInit() {
    await this.producer.connect();
  }

  async produce(record: ProducerRecord) {
    await this.producer.send(record);
  }

  async onApplicationShutdown() {
    await this.producer.disconnect();
  }
}
