import { Injectable } from '@nestjs/common';
import { ProducerService } from './kafka/producer.service';

@Injectable()
export class AppService {
  constructor(private readonly producerService: ProducerService) {}

  async produceMessage(message: string) {
    await this.producerService.produce({
      topic: 'new',
      messages: [
        {
          value: message,
        },
      ],
    });
    return 'Hello World!';
  }
}
