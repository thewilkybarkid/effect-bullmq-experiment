import { NodeRuntime } from '@effect/platform-node'
import { Config, Effect, LogLevel, Logger, Random, Schedule } from 'effect'
import * as BullMq from './BullMq.js'
import { RedisConfig } from './Redis.js'

const redisConfig: Config.Config<RedisConfig> = Config.nested(
  Config.all({
    url: Config.mapAttempt(Config.string('URL'), url => new URL(url)),
    family: Config.integer('IP_VERSION').pipe(Config.withDefault(4)),
  }),
  'REDIS',
)

const AddToQueue = Effect.gen(function* (_) {
  const number = yield* _(Random.nextInt)

  yield* _(BullMq.AddJob('some-job', { number }, { attempts: 2, removeOnComplete: true, removeOnFail: true }))
}).pipe(
  Effect.repeat(Schedule.jittered(Schedule.spaced('2 seconds'))),
  Effect.provideServiceEffect(BullMq.Queue, BullMq.CreateQueue('jobs')),
  Effect.scoped,
)

const ProcessQueue = BullMq.Worker(
  'jobs',
  job =>
    Effect.gen(function* (_) {
      const outcome = yield* _(Random.nextIntBetween(1, 4))

      yield* _(Effect.logDebug('Doing some processing'), Effect.annotateLogs('data', job.data))

      switch (outcome) {
        case 1:
          return BullMq.Outcome.Completed(undefined)
        case 2:
          return BullMq.Outcome.Failure({ cause: new Error('some failure') })
        default:
          const delay = yield* _(Random.nextIntBetween(1, 10))

          return BullMq.Outcome.Delayed({ delay: `${delay} seconds` })
      }
    }),
  Schedule.spaced('1 seconds'),
)

const Program = Effect.all([ProcessQueue, AddToQueue], { concurrency: 'unbounded' }).pipe(
  Effect.forever,
  Effect.provideServiceEffect(RedisConfig, redisConfig),
)

Program.pipe(Effect.tapErrorCause(Effect.logError), Logger.withMinimumLogLevel(LogLevel.Debug), NodeRuntime.runMain)
