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

test('batch - reads its own puts and deletes', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  await db.put('a', 1)
  await db.put('b', 2)

  const batch = db.batch()

  batch.put('c', 3)
  batch.del('a')
  batch.put('b', 22)

  t.is((await batch.get('c')).value, 3)
  t.is(await batch.get('a'), null)
  t.is((await batch.get('b')).value, 22)
  t.is(await batch.get('missing'), null)

  const entries = []
  for await (const entry of batch.createReadStream()) {
    entries.push([entry.key, entry.value])
  }
  t.alike(entries, [
    ['b', 22],
    ['c', 3]
  ])

  t.alike(await batch.peek({ reverse: true }), { seq: 0, key: 'c', value: 3 })

  await batch.flush()

  t.is(await db.get('a'), null)
  t.is((await db.get('b')).value, 22)
  t.is((await db.get('c')).value, 3)
})

test('batch - overwriting the same key repeatedly, last write wins', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  const batch = db.batch()

  batch.put('a', 1)
  batch.put('a', 2)
  batch.del('a')
  batch.put('a', 3)

  t.is((await batch.get('a')).value, 3)

  batch.del('a')
  t.is(await batch.get('a'), null)

  await batch.flush()
  t.is(await db.get('a'), null)
})

test('batch - range options and clear', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  for (const k of ['a', 'b', 'c']) await db.put(k, k)

  const batch = db.batch()
  batch.put('d', 'd')
  batch.del('b')

  const keys = []
  for await (const entry of batch.createReadStream({ gte: 'b', lt: 'e' })) keys.push(entry.key)
  t.alike(keys, ['c', 'd'])

  const limited = []
  for await (const entry of batch.createReadStream({ reverse: true, limit: 2 })) {
    limited.push(entry.key)
  }
  t.alike(limited, ['d', 'c'])

  batch.clear()
  t.is(await batch.get('a'), null)

  batch.put('z', 'z')
  t.is((await batch.get('z')).value, 'z')

  const after = []
  for await (const entry of batch.createReadStream()) after.push(entry.key)
  t.alike(after, ['z'])

  await batch.close()
})

test('batch - sub keeps prefix on reads', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })
  const sub = db.sub('s')

  await sub.put('a', 1)

  const batch = sub.batch()
  batch.put('b', 2)

  t.is((await batch.get('a')).value, 1)
  t.is((await batch.get('b')).value, 2)

  const keys = []
  for await (const entry of batch.createReadStream()) keys.push(entry.key)
  t.alike(keys, ['a', 'b'])

  await batch.flush()
})

test('wrapper - per-call encoding override with a non-string value', async function (t) {
  const db = await create(t)

  await db.put('a', { n: 1 }, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  const entry = await db.get('a', { keyEncoding: 'utf-8', valueEncoding: 'json' })
  t.alike(entry, { seq: 0, key: 'a', value: { n: 1 } })

  await db.del('a', { keyEncoding: 'utf-8' })
  t.is(await db.get('a', { keyEncoding: 'utf-8' }), null)
})

test('wrapper - entries are plain seq/key/value', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  await db.put('a', 1)

  t.alike(await db.get('a'), { seq: 0, key: 'a', value: 1 })
  t.alike(await db.peek(), { seq: 0, key: 'a', value: 1 })

  const entry = await db.get('a')
  t.is(JSON.stringify(entry), '{"seq":0,"key":"a","value":1}')
})

test('batch - honours batch level encodings', async function (t) {
  const db = await create(t)

  const batch = db.batch({ keyEncoding: 'utf-8', valueEncoding: 'json' })
  batch.put('a', { n: 1 })

  t.alike(await batch.get('a'), { seq: 0, key: 'a', value: { n: 1 } })

  const entries = []
  for await (const entry of batch.createReadStream()) entries.push(entry)
  t.alike(entries, [{ seq: 0, key: 'a', value: { n: 1 } }])

  await batch.flush()

  t.alike(await db.get('a', { keyEncoding: 'utf-8', valueEncoding: 'json' }), {
    seq: 0,
    key: 'a',
    value: { n: 1 }
  })
})

test('batch - core length counts pending puts', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  await db.put('a', 1)

  const batch = db.batch()
  t.is(batch.core.length, 1)

  batch.put('b', 2)
  batch.del('a')
  t.is(batch.core.length, 2)

  await batch.close()
})

test('batch - reads throw once flushed', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  const batch = db.batch()
  batch.put('a', 1)
  await batch.flush()

  await t.exception(batch.get('a'))
  t.exception(() => batch.createReadStream())
  await t.exception(batch.peek())
})

test('batch - diff stream matches the wrapper and sees pending ops', async function (t) {
  const db = await create(t, { keyEncoding: 'utf-8', valueEncoding: 'json' })

  await db.put('a', 1)
  await db.put('b', 2)

  const snap = db.snapshot()

  await db.put('c', 3)
  await db.put('a', 11)

  const empty = db.batch()
  t.alike(
    await collectDiff(empty.createDiffStream(snap)),
    await collectDiff(db.createDiffStream(snap))
  )
  await empty.close()

  const batch = db.batch()
  batch.put('d', 4)
  batch.del('b')

  t.alike(await collectDiff(batch.createDiffStream(snap)), [
    [
      ['a', 11],
      ['a', 1]
    ],
    [null, ['b', 2]],
    [['c', 3], null],
    [['d', 4], null]
  ])

  await batch.close()
})

async function collectDiff(stream) {
  const out = []
  for await (const diff of stream) {
    out.push([
      diff.left && [diff.left.key, diff.left.value],
      diff.right && [diff.right.key, diff.right.value]
    ])
  }
  return out
}

async function create(t, opts) {
  const store = new Corestore(await t.tmp())
  const bee = new Hyperbee2(store)
  t.teardown(() => bee.close())
  return new Wrapper(bee, opts)
}
