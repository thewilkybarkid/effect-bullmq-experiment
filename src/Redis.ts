import { Context } from 'effect'

export interface RedisConfig {
  readonly family: number
  readonly url: URL
}

export const RedisConfig = Context.GenericTag<RedisConfig>('RedisConfig')
