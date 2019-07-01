const _ = require('lodash')
const ms = require('ms')
const Id = require('@hotelflex/id')
const { transaction } = require('objection')
const { Broker, Handler } = require('@hotelflex/amqp')

const AMQP_PROCESSED = 'amqp_processed_messages'
const AMQP_OUTBOUND = 'amqp_outbound_messages'

const unixToISO = time => (time ? new Date(time).toISOString() : null)

const parseMsg = msg => ({
  id: msg.properties.messageId,
  key: msg.fields.routingKey,
  body: JSON.parse(msg.content.toString()),
  timestamp: unixToISO(msg.properties.timestamp),
  correlationId: msg.properties.correlationId,
})

const hasArgs = (rabbitmq, knexClient, queues) =>
  Boolean(rabbitmq && rabbitmq.host && rabbitmq.port && knexClient && queues)

class MessageConsumer {
  async start(rabbitmq, knexClient, queues, logger) {
    try {
      if (!hasArgs(rabbitmq, knex, queues))
        throw new Error('Missing required arguments')
      this.rabbitmq = rabbitmq
      this.knex = knexClient
      this.queues = queues
      this.log = logger || console
      await Broker.connect(
        this.rabbitmq,
        this.log,
      )
      this.registerQueues()
    } catch (err) {
      this.log.error(
        {
          error: err.message,
        },
        '[MessageConsumer] Error starting up',
      )
    }
  }
  registerQueues() {
    this.queues.forEach(({ queue, binding, action, ...opts }) => {
      /*
      * ----- DEFINE HANDLER METHOD ------
      */

      const handle = msg =>
        transaction(this.knex, async trx => {
          let message
          const start = Date.now()
          try {
            /*
            * ---CHECK IF ALREADY PROCESSED ------
            */

            message = parseMsg(msg)
            const doc = await trx(AMQP_PROCESSED)
              .where('id', message.id)
              .first('id')
            if (doc) {
              this.log.info(
                {
                  queue,
                  ...message,
                },
                'Message already processed',
              )
              return
            }
            /*
            * ------- AWAIT ACTION ------
            */

            const messages = await action(trx, message.body)

            /*
            * ---- CHECK FOR OUTBOUND MESSAGES ---
            */

            if (messages) {
              await trx(AMQP_OUTBOUND)
                .returning('id')
                .insert(
                  messages.map(m => ({
                    id: Id.create(),
                    key: m.key,
                    body: JSON.stringify(m.body),
                    timestamp: new Date().toISOString(),
                    //include correlationId from current
                    correlationId: message.correlationId,
                  })),
                )
            }
            /*
            * ---- INSERT TO PROCESSED MESSAGES ---
            */

            await trx(AMQP_PROCESSED)
              .returning('id')
              .insert({
                id: message.id,
                key: message.key,
                body: JSON.stringify(message.body),
                timestamp: message.timestamp,
                correlationId: message.correlationId,
                processedAt: new Date().toISOString(),
              })

            this.log.info(
              {
                duration: ms(Date.now() - start),
                queue,
                ...message,
              },
              'Message processing succeeded',
            )
          } catch (err) {
            if (err.message.indexOf(`${AMQP_PROCESSED}_pkey`) > -1) {
              this.log.info(
                {
                  queue,
                  ...message,
                },
                'Message already processed',
              )
            } else {
              this.log.error(
                {
                  duration: ms(Date.now() - start),
                  queue,
                  ...message,
                  error: err.message,
                },
                'Message processing failed',
              )
              throw err
            }
          }
        })

      /*
      * ----- END OF HANDLER METHOD ------
      */

      Broker.registerHandler(
        new Handler(
          _.omitBy(
            {
              ...opts,
              queue,
              binding,
              action: handle,
            },
            _.isNil,
          ),
          this.log,
        ),
      )
    })
  }
}

module.exports = new MessageConsumer()
