import { Body, Controller, Get, Param, Post, Query } from '@nestjs/common';
import { AppService } from './app.service';
import { ApiProperty } from '@nestjs/swagger';

export class ProduceMessageDTO {
  @ApiProperty({
    default: {
      username: 'Duc Dau',
      totalPrice: 60,
      address: 'HCM',
    },
    nullable: true,
  })
  value: any;

  @ApiProperty({
    default: null,
    nullable: true,
  })
  key: string;
}

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Post('/simple')
  produce(@Body() { value, key }: ProduceMessageDTO) {
    return this.appService.produceMessage('simple_order', {
      value,
      key,
    });
  }

  @Post('/history')
  streamHistory(@Body() { value, key }: ProduceMessageDTO) {
    return this.appService.produceMessage('one_partition', {
      value,
      key,
    });
  }

  @Post('/registry')
  produceWithRegistry(@Body() { value, key }: ProduceMessageDTO) {
    return this.appService.produceMessageWithRegistry('order_with_registry', {
      value,
      key,
    });
  }
}
