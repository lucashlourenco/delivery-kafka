const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'notification-group' });

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'order-status-events', fromBeginning: false });

  console.log("🔔 Serviço de Notificações aguardando eventos...");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      
      console.log("\n--- NOTIFICAÇÃO EM TEMPO REAL ---");
      console.log(`📱 Usuário, o status do seu pedido #${data.orderId} mudou!`);
      console.log(`📢 Status: ${data.status}`);
      console.log(`💬 "${data.message}"`);
      console.log("---------------------------------\n");
    },
  });
}

run().catch(console.error);