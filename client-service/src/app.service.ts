import { Injectable } from '@nestjs/common';
import { ProducerService } from './kafka/producer.service';
import { Message } from 'kafkajs';

@Injectable()
export class AppService {
  constructor(private readonly producerService: ProducerService) {}

  async produceMessage(message: Message) {
    return await this.producerService.produce({
      topic: 'two_partition',
      messages: [message],
    });
  }
}
