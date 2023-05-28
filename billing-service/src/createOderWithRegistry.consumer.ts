import { Injectable, OnModuleInit } from '@nestjs/common';
import { ConsumerService } from './kafka/consumer.service';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

@Injectable()
export class CreateOrderConsumerWithRegistry implements OnModuleInit {
  constructor(private readonly consumerService: ConsumerService) {}
  private readonly registry = new SchemaRegistry({
    host: 'https://psrc-e8vk0.southeastasia.azure.confluent.cloud',
    auth: {
      username: 'F6ROZMFWSQKIBMLR',
      password:
        'fro+F3ch2O0LOqGXun2Om3ocyDxToL1y5aaM/A1fcKj3LuLg6IYuewzLoNU5dGq+',
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
                beforeDecode: message.value,
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
