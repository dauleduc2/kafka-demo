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
    host: 'https://psrc-znpo0.ap-southeast-2.aws.confluent.cloud',
    auth: {
      username: 'V7XDP3DGHUDNAERW',
      password:
        'N5JgVGWrPqENmrrX9KegCvsZlRRCgie8dtaouk7psu0nagkIxY36wllW3hin8dNK',
    },
    clientId: 'demo',
  });
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
  private schema;
  private registryId;
  async onModuleInit() {
    this.schema = await readAVSCAsync(
      path.join(__dirname, '..', '..', 'schema.avsc'),
    );

    const { id } = await this.registry.register(this.schema);
    this.registryId = id;

    await this.producer.connect();
  }

  async produce(record: ProducerRecord) {
    try {
      const encodedValue = await this.registry.encode(
        this.registryId,
        record.messages[0].value,
      );
      await this.producer.send({
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

  async onApplicationShutdown() {
    await this.producer.disconnect();
  }
}
