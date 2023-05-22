import { Body, Controller, Get, Param, Query } from '@nestjs/common';
import { AppService } from './app.service';

export class ProduceMessageDTO {
  topic: string;
  value: string;
  key: string;
}

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get()
  produce(@Body() { topic, value, key }: ProduceMessageDTO) {
    return this.appService.produceMessage(topic, {
      value,
      key,
    });
  }
}
