import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import {
  Admin,
  Consumer,
  ConsumerRunConfig,
  ConsumerSubscribeTopic,
  Kafka,
} from 'kafkajs';

@Injectable()
export class AdminService implements OnApplicationShutdown {
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

  private readonly admin: Admin = this.kafka.admin();

  async onModuleInit() {
    await this.admin.connect();
  }

  async createTopic(topic: string) {
    await this.admin.createTopics({
      topics: [{ topic, numPartitions: 3, replicationFactor: 1 }],
    });
  }

  async listTopics() {
    const topics = await this.admin.listTopics();
    console.log(topics);
  }

  async deleteTopic(topic: string) {
    await this.admin.deleteTopics({
      topics: [topic],
    });
  }

  async describeTopic(topic: string) {
    const topicMetadata = await this.admin.fetchTopicMetadata({
      topics: [topic],
    });
    console.log(topicMetadata);
  }

  async fetchOffsets(topic: string) {
    const offsets = await this.admin.fetchTopicOffsets(topic);
    console.log(offsets);
  }

  async onApplicationShutdown() {
    await this.admin.disconnect();
  }
}
