import { Body, Controller, Get, Param, Query } from '@nestjs/common';
import { AppService } from './app.service';

export class ProduceMessageDTO {
  value: string;
  key: string;
}

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get()
  produce(@Body() { value, key }: ProduceMessageDTO) {
    return this.appService.produceMessage({
      value,
      key,
    });
  }
}
