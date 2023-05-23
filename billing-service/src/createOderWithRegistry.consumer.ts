import { Injectable, OnModuleInit } from '@nestjs/common';
import { ConsumerService } from './kafka/consumer.service';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

@Injectable()
export class CreateOrderConsumerWithRegistry implements OnModuleInit {
  constructor(private readonly consumerService: ConsumerService) {}
  private readonly registry = new SchemaRegistry({
    host: 'https://psrc-10wgj.ap-southeast-2.aws.confluent.cloud',
    auth: {
      username: 'UESJT6BVUOPSIZQV',
      password:
        'QWcdHqGzrzT4m1SMFGcbSZjMQfhpdlISj9M/M2PV558jca6QjFzqMhJJimi/1cPJ',
    },
    clientId: 'demo',
  });

  async onModuleInit() {
    await this.consumerService.consume(
      { topic: 'order_with_registry' },
      {
        eachMessage: async ({ topic, partition, message }) => {
          await new Promise((resolve) =>
            setTimeout(async () => {
              const decodedValue = await this.registry.decode(message.value);
              console.log({
                value: decodedValue,
                topic: topic.toString(),
                partition: partition.toString(),
              });
              resolve('done');
            }, 500),
          );
        },
      },
    );
  }
}
