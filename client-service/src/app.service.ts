import { Injectable } from '@nestjs/common';
import { ProducerService } from './kafka/producer.service';
import { Message } from 'kafkajs';

@Injectable()
export class AppService {
  constructor(private readonly producerService: ProducerService) {}

  async produceMessage(topic: string, message: Message) {
    return await this.producerService.produce({
      topic,
      messages: [message],
    });
  }

  async produceMessageWithRegistry(topic: string, message: Message) {
    return await this.producerService.produceWithRegistry({
      topic,
      messages: [message],
    });
  }
}
