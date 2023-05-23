import { Body, Controller, Get, Param, Query } from '@nestjs/common';
import { AppService } from './app.service';

export class ProduceMessageDTO {
  topic: string;
  value: any;
  key: string;
}

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get('/simple')
  produce(@Body() { topic, value, key }: ProduceMessageDTO) {
    return this.appService.produceMessage(topic, {
      value,
      key,
    });
  }

  @Get('/registry')
  produceWithRegistry(@Body() { topic, value, key }: ProduceMessageDTO) {
    return this.appService.produceMessageWithRegistry(topic, {
      value,
      key,
    });
  }
}
