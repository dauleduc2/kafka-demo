import { Injectable, OnModuleInit } from '@nestjs/common';
import { ConsumerService } from './kafka/consumer.service';

@Injectable()
export class CreateOrderConsumerFromStart implements OnModuleInit {
  constructor(private readonly consumerService: ConsumerService) {}

  async onModuleInit() {
    await this.consumerService.consume(
      { topic: 'one_partition', fromBeginning: true },
      {
        autoCommit: false,
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

    // await this.consumerService.seek('one_partition', 0, '0', {
    //   eachBatch: async ({ batch }) => {
    //     for (const message of batch.messages) {
    //       await new Promise((resolve) =>
    //         setTimeout(async () => {
    //           console.log({
    //             value: JSON.parse(message.value.toString()),
    //             topic: batch.topic,
    //             partition: batch.partition,
    //           });
    //           resolve('done');
    //         }, 500),
    //       );
    //     }
    //   },
    // });
  }
}
