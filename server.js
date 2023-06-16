const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const amqp = require("amqplib");

const app = express();

app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));

app.get('/status', (request, response) => response.json({clients: clients.length}));

const PORT = 3500;

let clients = [];
let facts = [];

function eventsHandler(request, response, next) {
    const headers = {
      'Content-Type': 'text/event-stream',
      'Connection': 'keep-alive',
      'Cache-Control': 'no-cache',
      'Access-Control-Allow-Origin': 'http://localhost:4200'
    };
    response.writeHead(200, headers);
  
    const data = `data: ${JSON.stringify(facts)}\n\n`;
  
    response.write(data);
  
    const clientId = Date.now();
  
    const newClient = {
      id: clientId,
      response
    };
  
    clients.push(newClient);
  
    request.on('close', () => {
      console.log(`${clientId} Connection closed`);
      clients = clients.filter(client => client.id !== clientId);
    });
  }
  // ...

function sendEventsToAll(newFact) {
    clients.forEach(client => client.response.write(`data: ${JSON.stringify(newFact)}\n\n`))
  }
  
  async function addFact(request, respsonse, next) {
    const newFact = request.body;
    facts.push(newFact);
    respsonse.json(newFact)
    return sendEventsToAll(newFact);
  }
  
  app.post('/fact', addFact);
  app.get('/events', eventsHandler);


(async () => {
  let connection;
  try {
    const connection = await amqp.connect("amqp://localhost:5672");
    const channel1 = await connection.createChannel();
    const channel2 = await connection.createChannel();

    process.once("SIGINT", async () => {
      await channel1.close();
      await channel2.close();
      await connection.close();
    });

    await channel1.assertQueue('nt8_pivotdiff', { durable: false });
    await channel1.consume(
      'nt8_pivotdiff',
      (message) => {
        console.log(message.content.toString());
        sendEventsToAll(message.content.toString());
      },
      { noAck: false }
    );

    await channel2.assertQueue('nt8_prevohlc', { durable: false });
    await channel2.consume(
      'nt8_prevohlc',
      (message) => {
        console.log(message.content.toString());
        sendEventsToAll(message.content.toString());
      },
      { noAck: false }
    );

  } catch (err) {
    console.warn(err);
  } finally {
    if (connection) await connection.close();
  }
})();


app.listen(PORT, () => {
  console.log(`Facts Events service listening at http://localhost:${PORT}`)
})
