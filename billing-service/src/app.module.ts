import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { KafkaModule } from './kafka/kafka.module';
import { CreateOrderConsumer } from './createOder.consumer';
import { CreateOrderConsumerWithRegistry } from './createOderWithRegistry.consumer';

@Module({
  imports: [KafkaModule],
  controllers: [AppController],
  providers: [AppService, CreateOrderConsumer],
})
export class AppModule {}
