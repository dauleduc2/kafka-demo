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
    host: 'https://psrc-10wgj.ap-southeast-2.aws.confluent.cloud',
    auth: {
      username: 'UESJT6BVUOPSIZQV',
      password:
        'QWcdHqGzrzT4m1SMFGcbSZjMQfhpdlISj9M/M2PV558jca6QjFzqMhJJimi/1cPJ',
    },
    clientId: 'demo',
  });

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
