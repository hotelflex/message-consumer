const _ = require('lodash')
const rabbit = require('rabbot')
const ms = require('ms')
const Id = require('@hotelflex/id')
const { createDbPool, Errors } = require('@hotelflex/db-utils')

const getMetadata = headers => ({
  operationId: headers['operation-id'] || Id.create(),
  transactionId: headers['transaction-id'] || Id.create(),
})

const hasArgs = (config={}) => Boolean(
  config.rabbitmq
  && config.postgres
  && config.rabbitmq.host 
  && config.rabbitmq.port 
  && config.rabbitmq.exchange
  && config.postgres.database
  && config.postgres.host
  && config.postgres.port
)

class MessageConsumer {
  constructor() {
    this.queues = []
    this.start = this.start.bind(this)
    this.connect = this.connect.bind(this)
    this.registerQueues = this.registerQueues.bind(this)
  }
  start(config={}, logger) {
    this.rabbitmq = config.rabbitmq
    this.postgres = config.postgres
    this.logger = logger || console
    this.opsTable = config.opsTable || 'operations'
    this.disableReconnect = config.disableReconnect || false
    if(!hasArgs(config)) throw new Error('Missing required arguments.')
    this.db = createDbPool(this.postgres, { min: 2, max: 8 })
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

            /*
            * ---- CHECK FOR DUPLICATE OPERATION -------
            */

            const opId = message.properties.headers['operation-id']
            const op = await this.db(this.opsTable)
              .where('id', opId)
              .first('id')
            if(op) throw new Errors.DuplicateOperation()

            /*
            * ---------- ATTEMPT ACTION ------------
            */

            await action(
              message.body,
              getMetadata(message.properties.headers),
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

    rabbit.on('connected', () => {
      this.logger.debug('RabbitMQ - Connected.')
    })
    rabbit.on('failed', () => {
      if(this.disableReconnect) process.exit()
      this.logger.debug('RabbitMQ - Connection lost, reconnecting...')
    })
    rabbit.on('closed', () => {
      //intentional - no reason for this to happen
      this.logger.info('RabbitMQ - Connection closed.')
      process.exit()
    })
    rabbit.on('unreachable', () => {
      //unintentional - reconnection attempts have failed
      this.logger.error('RabbitMQ - Connection failed.')
      process.exit()
    })

    /*
    * -------------- CONNECT --------------
    * Note: queues must be registered before connecting
    */

    rabbit.configure({
      connection: Object.assign(
        { name: 'default', replyQueue: false },
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
        limit: q.limit,
        priority: q.priority,
        noBatch: q.noBatch,
      }, _.isNil))),
      bindings: this.queues.map(q => ({
        exchange: this.rabbitmq.exchange,
        target: q.channel,
        keys: [q.binding],
      })),
    })
  }
}

module.exports = new MessageConsumer()