// SPDX-License-Identifier: CC0-1.0

import { randomUUID } from 'node:crypto'
import {
  Admin as BaseAdmin,
  Consumer as BaseConsumer,
  Producer as BaseProducer,
  type AdminOptions as BaseAdminOptions,
  type ConsumerOptions as BaseConsumerOptions,
  type ProducerOptions as BaseProducerOptions,
} from '@platformatic/kafka'

export {
  stringSerializers,
  stringDeserializers,
  AclOperations,
  AclPermissionTypes,
  ResourcePatternTypes,
  ResourceTypes,
} from '@platformatic/kafka'

type WithGcnCredentials<T> = Omit<T, 'bootstrapBrokers' | 'sasl' | 'tls'> &
  GcnKafkaConfig

type ConsumerOptions = Omit<
  WithGcnCredentials<BaseConsumerOptions<Buffer, Buffer, Buffer, Buffer>>,
  'groupId'
> & {
  groupId?: string
}

type ProducerOptions = WithGcnCredentials<
  BaseProducerOptions<Buffer, Buffer, Buffer, Buffer>
>

type AdminOptions = WithGcnCredentials<BaseAdminOptions>

type GcnKafkaConfig = {
  client_id?: string
  client_secret?: string
  domain?: 'gcn.nasa.gov' | 'test.gcn.nasa.gov' | 'dev.gcn.nasa.gov'
}

interface TokenProviderOptions {
  clientId: string
  clientSecret: string
  tokenEndpoint: string
  refreshMarginMs?: number
}

interface CachedToken {
  token: string
  expiresAt: number
}

function resolveOptions<T extends GcnKafkaConfig>(options: T) {
  const { client_id, client_secret, domain = 'gcn.nasa.gov', ...rest } = options
  if (!client_id || !client_secret) throw new Error()

  const token = createTokenProvider({
    clientId: client_id,
    clientSecret: client_secret,
    tokenEndpoint: `https://auth.${domain}/oauth2/token`,
  })

  return {
    ...rest,
    bootstrapBrokers: [`kafka.${domain}:9092`],
    tls: {},
    sasl: {
      mechanism: 'OAUTHBEARER',
      token,
    },
  }
}

function createTokenProvider(
  options: TokenProviderOptions,
): () => Promise<string> {
  const {
    clientId,
    clientSecret,
    tokenEndpoint,
    refreshMarginMs = 60_000,
  } = options

  let cached: CachedToken | undefined

  async function requestToken(): Promise<string> {
    const basic = Buffer.from(
      `${encodeURIComponent(clientId)}:${encodeURIComponent(clientSecret)}`,
    ).toString('base64')

    const response = await fetch(tokenEndpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Accept: 'application/json',
        Authorization: `Basic ${basic}`,
      },
      body: new URLSearchParams({ grant_type: 'client_credentials' }),
    })

    if (!response.ok) {
      const text = await response.text()
      throw new Error(
        `GCN OAuth token request failed (${response.status} ${response.statusText}): ${text}`,
      )
    }

    const body = (await response.json()) as {
      access_token?: string
      expires_in?: number
    }
    if (
      typeof body.access_token !== 'string' ||
      body.access_token.length === 0
    ) {
      throw new Error(
        'GCN OAuth token response did not contain an access_token',
      )
    }

    const lifetimeMs =
      (typeof body.expires_in === 'number' ? body.expires_in : 3600) * 1000
    cached = { token: body.access_token, expiresAt: Date.now() + lifetimeMs }
    return cached.token
  }

  return async function getToken(): Promise<string> {
    if (cached && Date.now() < cached.expiresAt - refreshMarginMs) {
      return cached.token
    }
    return await requestToken()
  }
}

export class Consumer extends BaseConsumer {
  constructor(options: ConsumerOptions) {
    const { groupId, ...rest } = options
    super({
      ...resolveOptions(rest),
      groupId: groupId ?? randomUUID(),
    } as BaseConsumerOptions<Buffer, Buffer, Buffer, Buffer>)
  }
}

export class Producer extends BaseProducer {
  constructor(options: ProducerOptions) {
    super(
      resolveOptions(options) as BaseProducerOptions<
        Buffer,
        Buffer,
        Buffer,
        Buffer
      >,
    )
  }
}

export class Admin extends BaseAdmin {
  constructor(options: AdminOptions) {
    super(resolveOptions(options) as BaseAdminOptions)
  }
}
