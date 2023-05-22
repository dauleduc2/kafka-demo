import { Injectable, OnModuleInit } from '@nestjs/common';
import { ConsumerService } from './kafka/consumer.service';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

@Injectable()
export class CreateOrderConsumer implements OnModuleInit {
  constructor(private readonly consumerService: ConsumerService) {}
  private readonly registry = new SchemaRegistry({
    host: 'https://psrc-znpo0.ap-southeast-2.aws.confluent.cloud',
    auth: {
      username: 'V7XDP3DGHUDNAERW',
      password:
        'N5JgVGWrPqENmrrX9KegCvsZlRRCgie8dtaouk7psu0nagkIxY36wllW3hin8dNK',
    },
    clientId: 'demo',
  });

  async onModuleInit() {
    // await this.consumerService.consume(
    //   { topic: 'one_partition', fromBeginning: true },
    //   {
    //     eachMessage: async ({ topic, partition, message }) => {
    //       await new Promise((resolve) =>
    //         setTimeout(async () => {
    //           const decodedKey = (await this.registry.decode(message.value))
    //             .test;
    //           console.log({
    //             value: decodedKey,
    //             topic: topic.toString(),
    //             partition: partition.toString(),
    //           });
    //           resolve('done');
    //         }, 500),
    //       );
    //     },
    //   },
    // );

    await this.consumerService.seek('one_partition', 0, '0', {
      eachBatch: async ({ batch, heartbeat }) => {
        for (const message of batch.messages) {
          const decodedKey = (await this.registry.decode(message.value)).test;
          console.log({
            value: decodedKey,
            topic: batch.topic,
            partition: batch.partition,
          });
          heartbeat();
        }
      },
    });
  }
}
