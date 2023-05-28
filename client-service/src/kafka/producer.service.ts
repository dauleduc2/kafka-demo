import {
  SchemaRegistry,
  readAVSCAsync,
} from '@kafkajs/confluent-schema-registry';
import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { Kafka, Producer, ProducerRecord } from 'kafkajs';
import * as path from 'path';

@Injectable()
export class ProducerService implements OnModuleInit, OnApplicationShutdown {
  private readonly registry = new SchemaRegistry({
    host: 'https://psrc-e8vk0.southeastasia.azure.confluent.cloud',
    auth: {
      username: 'F6ROZMFWSQKIBMLR',
      password:
        'fro+F3ch2O0LOqGXun2Om3ocyDxToL1y5aaM/A1fcKj3LuLg6IYuewzLoNU5dGq+',
    },
    clientId: 'demo',
  });

  private readonly kafka = new Kafka({
    clientId: 'demo',
    brokers: ['pkc-4j8dq.southeastasia.azure.confluent.cloud:9092'],
    ssl: true,
    sasl: {
      username: '47UTB7V3KQQDXHJR',
      password:
        'xN5dc2qr74RvYCxCRCRTl7u4tO3jvh9H7UjTaZPjXY1LTOS+tIFEeLukN/LxmX+X',
      mechanism: 'plain',
    },
  });

  private readonly producer: Producer = this.kafka.producer();
  private schema;
  private registryId;
  async onModuleInit() {
    this.schema = await readAVSCAsync(
      path.join(__dirname, '..', '..', 'demo_schema.avsc'),
    );

    const { id } = await this.registry.register(this.schema);
    this.registryId = id;

    await this.producer.connect();
  }

  async produceWithRegistry(record: ProducerRecord) {
    try {
      const encodedValue = await this.registry.encode(
        this.registryId,
        record.messages[0].value,
      );
      return await this.producer.send({
        ...record,
        messages: [
          {
            value: encodedValue,
          },
        ],
      });
    } catch (error) {
      return error;
    }
  }

  async produce(record: ProducerRecord) {
    try {
      return await this.producer.send({
        ...record,
        messages: [
          {
            key: record.messages[0].key,
            value: JSON.stringify(record.messages[0].value),
          },
        ],
      });
    } catch (error) {
      return error;
    }
  }

  async onApplicationShutdown() {
    await this.producer.disconnect();
  }
}
