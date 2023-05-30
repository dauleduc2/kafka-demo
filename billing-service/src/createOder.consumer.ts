import { Injectable, OnModuleInit } from '@nestjs/common';
import { ConsumerService } from './kafka/consumer.service';

@Injectable()
export class CreateOrderConsumer implements OnModuleInit {
  constructor(private readonly consumerService: ConsumerService) {}

  async onModuleInit() {
    await this.consumerService.consume(
      { topic: 'simple_order' },
      {
        eachMessage: async ({ topic, partition, message }) => {
          await new Promise((resolve) =>
            setTimeout(async () => {
              console.log({
                key: message.key?.toString(),
                value: JSON.parse(message.value.toString()),
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
