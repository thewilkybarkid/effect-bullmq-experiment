import * as BullMq from 'bullmq'
import { Clock, Context, Data, Duration, Effect, Random, Runtime, Schedule, Scope } from 'effect'
import * as Redis from './Redis.js'

export type Processor<R = never> = (job: BullMq.JobJson) => Effect.Effect<Outcome, never, R>

export type Outcome = Data.TaggedEnum<{
  Completed: {}
  Failure: { cause: Error }
  Delayed: { delay: Duration.DurationInput }
}>

export const Outcome = Data.taggedEnum<Outcome>()

export type Worker = BullMq.Worker<unknown, unknown>
export type Queue = BullMq.Queue<unknown, unknown>
export type Job = BullMq.Job<unknown, unknown>

export const Queue = Context.GenericTag<Queue>('BullMq/Queue')

export const CreateQueue = (name: string): Effect.Effect<Queue, never, Scope.Scope | Redis.RedisConfig> =>
  Effect.acquireRelease(
    Effect.gen(function* (_) {
      const config = yield* _(Redis.RedisConfig)
      const runtime = yield* _(Effect.runtime())

      const queue = new BullMq.Queue(name, {
        connection: {
          host: config.url.hostname,
          port: parseInt(config.url.port, 10),
          username: config.url.username,
          password: config.url.password,
          family: config.family,
        },
      })

      queue.on('waiting', job =>
        Runtime.runSync(runtime)(
          Effect.logDebug('Job added to queue').pipe(Effect.annotateLogs({ job: job.id, jobName: job.name })),
        ),
      )

      return queue
    }),
    queue => Effect.promise(() => queue.close()),
  ).pipe(Effect.annotateLogs('queue', name))

export const Worker = <B, R1 = never, R2 = never>(
  queue: string,
  processor: Processor<R1>,
  schedule: Schedule.Schedule<B, void, R2>,
): Effect.Effect<void, never, Redis.RedisConfig | R1 | R2> =>
  Effect.acquireUseRelease(
    Effect.gen(function* (_) {
      const config = yield* _(Redis.RedisConfig)
      const runtime = yield* _(Effect.runtime<R1>())

      const worker: Worker = new BullMq.Worker<unknown, unknown>(queue, undefined, {
        autorun: false,
        connection: {
          host: config.url.hostname,
          port: parseInt(config.url.port, 10),
          username: config.url.username,
          password: config.url.password,
          family: config.family,
        },
      })

      worker.on('ready', () => Runtime.runSync(runtime)(Effect.logDebug('Worker ready')))
      worker.on('drained', () => Runtime.runSync(runtime)(Effect.logDebug('Worker has nothing to do')))
      worker.on('closing', () => Runtime.runSync(runtime)(Effect.logDebug('Worker closing')))
      worker.on('closed', () => Runtime.runSync(runtime)(Effect.logDebug('Worker closed')))

      return worker
    }),
    worker =>
      Effect.gen(function* (_) {
        const token = (yield* _(Random.nextInt)).toString()
        const job: BullMq.Job | undefined = yield* _(Effect.promise(() => worker.getNextJob(token)))

        if (!job) {
          return
        }

        yield* _(
          Effect.gen(function* (_) {
            yield* _(Effect.logDebug('Job active'))

            const outcome = yield* _(processor(job.asJSON()))

            switch (outcome._tag) {
              case 'Completed':
                yield* _(Effect.promise(() => job.moveToCompleted(undefined, token)))
                yield* _(Effect.logDebug('Job completed'))
                return
              case 'Failure':
                yield* _(Effect.promise(() => job.moveToFailed(outcome.cause, token)))
                yield* _(Effect.logDebug('Job failed'), Effect.annotateLogs('reason', job.failedReason))
                return
              case 'Delayed':
                const timestamp = yield* _(Clock.currentTimeMillis)
                yield* _(Effect.promise(() => job.moveToDelayed(timestamp + Duration.toMillis(outcome.delay), token)))
                yield* _(Effect.logDebug('Job delayed'), Effect.annotateLogs('delay', Duration.format(outcome.delay)))
                return
            }
          }),
          Effect.annotateLogs({ job: job.id, jobName: job.name, attempt: job.attemptsStarted }),
        )
      }).pipe(Effect.repeat(schedule)),
    worker => Effect.promise(() => worker.close()),
  ).pipe(Effect.annotateLogs('queue', queue), Effect.scoped)

export const AddJob = (name: string, data: unknown, options?: BullMq.JobsOptions): Effect.Effect<Job, never, Queue> =>
  Effect.gen(function* (_) {
    const queue = yield* _(Queue)

    return yield* _(Effect.promise(() => queue.add(name, data, options)))
  })
