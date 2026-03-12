const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'delivery-simulator',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

const delay = (ms) => new Promise(res => setTimeout(res, ms));

async function simulateOrder(orderId) {
  await producer.connect();
  console.log(`\n🚀 Iniciando acompanhamento do Pedido: ${orderId}`);

  const statuses = [
    { status: 'RECEBIDO', msg: 'O restaurante recebeu seu pedido!' },
    { status: 'PREPARANDO', msg: 'Seu lanche está na chapa!' },
    { status: 'SAIU_PARA_ENTREGA', msg: 'O entregador já está a caminho!' },
    { status: 'ENTREGUE', msg: 'Bom apetite! Pedido entregue.' }
  ];

  for (const step of statuses) {
    const payload = {
      orderId,
      status: step.status,
      message: step.msg,
      timestamp: new Date().toISOString()
    };

    // Usamos o orderId como 'key' para garantir que mensagens do mesmo pedido 
    // caiam na mesma partição do Kafka (mantendo a ordem cronológica)
    await producer.send({
      topic: 'order-status-events',
      messages: [
        { key: orderId, value: JSON.stringify(payload) },
      ],
    });

    console.log(`[EVENTO ENVIADO]: ${step.status}`);
    await delay(4000); // Espera 4 segundos entre as atualizações
  }

  console.log(`✅ Fluxo do pedido ${orderId} finalizado.\n`);
}

simulateOrder(`PEDIDO-${Math.floor(Math.random() * 1000)}`);