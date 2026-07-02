const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const Hyperbee2 = require('hyperbee2')

const Wrapper = require('../')

test('wrapper - encodings on get/put', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  await db.put('hello', { world: true })

  const entry = await db.get('hello')
  t.is(entry.key, 'hello')
  t.alike(entry.value, { world: true })

  t.is(await db.get('missing'), null)
})

test('wrapper - per-call encoding overrides', async function (t) {
  const db = await create(t)

  await db.put('a', '{"n":1}', { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })

  const entry = await db.get('a', { keyEncoding: 'utf-8', valueEncoding: 'json' })
  t.is(entry.key, 'a')
  t.alike(entry.value, { n: 1 })
})

test('wrapper - createReadStream encodes range and decodes entries', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  const batch = db.batch()
  for (const k of ['a', 'b', 'c', 'd']) batch.put(k, { k })
  await batch.flush()

  const entries = []
  for await (const entry of db.createReadStream({ gte: 'b', lt: 'd' })) {
    entries.push(entry)
  }

  t.alike(
    entries.map((e) => e.key),
    ['b', 'c']
  )
  t.alike(entries[0].value, { k: 'b' })

  const reversed = []
  for await (const entry of db.createReadStream({ reverse: true, limit: 2 })) {
    reversed.push(entry.key)
  }
  t.alike(reversed, ['d', 'c'])
})

test('wrapper - peek', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  await db.put('a', 1)
  await db.put('b', 2)

  const entry = await db.peek({ gte: 'b' })
  t.is(entry.key, 'b')
  t.is(entry.value, 2)
})

test('wrapper - sub prefixes and isolates entries', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  const animals = db.sub('animals')
  const plants = db.sub('plants')

  await animals.put('cat', 'meow')
  await plants.put('rose', 'red')
  await db.put('top', 'level')

  t.is((await animals.get('cat')).value, 'meow')
  t.is(await animals.get('rose'), null)
  t.is((await plants.get('rose')).value, 'red')

  const animalKeys = []
  for await (const entry of animals.createReadStream()) animalKeys.push(entry.key)
  t.alike(animalKeys, ['cat'])

  const plantKeys = []
  for await (const entry of plants.createReadStream()) plantKeys.push(entry.key)
  t.alike(plantKeys, ['rose'])

  // raw view of the shared tree sees prefixed keys
  const raw = []
  for await (const entry of new Wrapper(db.bee).createReadStream()) {
    raw.push(b4a.toString(entry.key))
  }
  t.alike(raw, ['animals\x00cat', 'plants\x00rose', 'top'])
})

test('wrapper - nested subs', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })

  const a = db.sub('a')
  const b = a.sub('b')

  await b.put('key', 'nested')
  await a.put('key', 'shallow')

  t.is((await b.get('key')).value, 'nested')
  t.is((await a.get('key')).value, 'shallow')

  const keys = []
  for await (const entry of a.createReadStream()) keys.push(entry.key)
  t.alike(keys, ['b\x00key', 'key'])
})

test('wrapper - sub ranges stay within prefix', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })

  const sub = db.sub('s')
  await db.put('a', 'out')
  await sub.put('a', 'in-a')
  await sub.put('b', 'in-b')
  await db.put('z', 'out')

  const keys = []
  for await (const entry of sub.createReadStream({ gte: 'a', lte: 'b' })) {
    keys.push(entry.key)
  }
  t.alike(keys, ['a', 'b'])

  const peeked = await sub.peek({ reverse: true })
  t.is(peeked.key, 'b')
})

test('wrapper - del', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })

  await db.put('a', '1')
  await db.del('a')

  t.is(await db.get('a'), null)
})

test('wrapper - snapshot keeps prefix and encodings', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })
  const sub = db.sub('s')

  await sub.put('a', 1)

  const snap = sub.snapshot()
  await sub.put('b', 2)

  t.is((await snap.get('a')).value, 1)
  t.is(await snap.get('b'), null)
  t.is((await sub.get('b')).value, 2)
})

async function create(t, opts) {
  const store = new Corestore(await t.tmp())
  const bee = new Hyperbee2(store)
  t.teardown(() => bee.close())
  return new Wrapper(bee, opts)
}
