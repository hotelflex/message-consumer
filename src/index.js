const _ = require('lodash')
const rabbit = require('rabbot')
const ms = require('ms')
const Id = require('@hotelflex/id')
const { Errors } = require('@hotelflex/db-utils')

const getMetadata = headers => ({
  operationId: headers['operation-id'] || Id.create(),
  transactionId: headers['transaction-id'] || Id.create(),
})
const getSession = () => ({ isRoot: true })

class MessageConsumer {
  constructor() {
    this.queues = []
    this.start = this.start.bind(this)
    this.connect = this.connect.bind(this)
    this.registerConsumers = this.registerConsumers.bind(this)
  }
  start(config={}, logger) {
    const { name, rabbitmq } = config
    this.name = name
    this.rabbitmq = rabbitmq
    this.logger = logger || console
    if (!name || !rabbitmq.host || !rabbitmq.port || !rabbitmq.exchange)
      throw new Error('Missing required arguments.')
    this.connect()
  }
  registerQueues(queues=[]) {
    this.queues = queues
    this.queues.forEach(({ channel, action }) => {
      rabbit.handle({
        queue: channel,
        type: '#',
        handler: async message => {
          const start = Date.now()

          try {
            await action(
              message.body,
              getMetadata(message.properties.headers),
              getSession(),
            )

            const end = Date.now()
            const duration = ms(end - start)

            this.logger.info(
              {
                duration,
                channel,
                headers: message.properties.headers,
                body: message.body,
              },
              'Message processing succeeded',
            )

            message.ack()
          } catch (err) {
            const end = Date.now()
            const duration = ms(end - start)

            if (err instanceof Errors.DuplicateOperation) {
              this.logger.info(
                {
                  duration,
                  channel,
                  headers: message.properties.headers,
                  body: message.body,
                },
                'Message already processed',
              )

              message.ack()
            } else {
              this.logger.error(
                {
                  duration,
                  channel,
                  headers: message.properties.headers,
                  body: message.body,
                  error: err.message,
                  stack: err.stack,
                },
                'Message processing failed',
              )

              message.nack()
            }
          }
        },
      })
    })
  }
  connect() {
    if(this.queues.length === 0) 
      throw new Error('No queues have been registered')

    /*
    * ------ HANDLE CONNECTION EVENTS ------
    */

    rabbit.on('closed', () => {
      this.logger.error('RabbitMQ connection closed')
      process.exit()
    })
    rabbit.on('unreachable', () => {
      this.logger.error('RabbitMQ connection unreachable')
      process.exit()
    })
    rabbit.on('failed', () => {
      this.logger.error('RabbitMQ connection failed')
      process.exit()
    })

    /*
    * -------------- CONNECT --------------
    * Note: consumers must be registered
    * before connecting
    */

    rabbit.configure({
      connection: Object.assign(
        { name: this.name, replyQueue: false },
        _.omit(this.rabbitmq, 'exchange'),
      ),
      exchanges: [
        {
          name: this.rabbitmq.exchange,
          type: 'topic',
          durable: true,
          persistent: true,
          publishTimeout: 2000,
        },
      ],
      queues: this.queues.map(q => (_.omitBy({
        name: q.channel,
        durable: true,
        subscribe: true,
        limit: q.prefetch,
        priority: q.priority,
        noBatch: q.noBatch,
      }, _.isNil))),
      bindings: this.queues.map((q => ({
        exchange: this.rabbitmq.exchange,
        target: q.channel,
        keys: [q.binding],
      })),
    })
  }
}

module.exports = new MessageConsumer()