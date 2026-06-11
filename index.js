const b4a = require('b4a')
const codecs = require('codecs')
const { Transform, pipeline } = require('streamx')

const SEP = b4a.alloc(1)
const EMPTY = b4a.alloc(0)

class WriteBatch {
  constructor(batch, encoding) {
    this.batch = batch
    this.encoding = encoding
  }

  tryPut(key, value) {
    this.batch.tryPut(enc(this.encoding.key, key), enc(this.encoding.value, value))
  }

  tryDelete(key) {
    this.batch.tryDelete(enc(this.encoding.key, key))
  }

  tryClear() {
    this.batch.tryClear()
  }

  lock() {
    return this.batch.lock()
  }

  flush() {
    return this.batch.flush()
  }

  close() {
    return this.batch.close()
  }
}

class Wrapper {
  constructor(bee, opts = {}) {
    this.bee = bee
    this.sep = toBuffer(opts.sep || SEP)
    this.prefix = opts.prefix ? toBuffer(opts.prefix) : null
    this.keyEncoding = opts.keyEncoding ? codecs(opts.keyEncoding) : null
    this.valueEncoding = opts.valueEncoding ? codecs(opts.valueEncoding) : null

    this._unprefixedKeyEncoding = this.keyEncoding
    this._sub = !!this.prefix
    this._autoClose = opts.autoClose !== false

    if (this.prefix) this.keyEncoding = prefixEncoding(this.prefix, this._unprefixedKeyEncoding)
  }

  get core() {
    return this.bee.core
  }

  head() {
    return this.bee.head()
  }

  ready() {
    return this.bee.ready()
  }

  close() {
    // subs share the underlying bee with their parent and don't own it
    return this._autoClose ? this.bee.close() : Promise.resolve()
  }

  replicate(...opts) {
    return this.bee.replicate(...opts)
  }

  update(root) {
    return this.bee.update(root)
  }

  sub(prefix, opts = {}) {
    let sep = opts.sep || this.sep
    if (!b4a.isBuffer(sep)) sep = b4a.from(sep)

    prefix = b4a.concat([this.prefix || EMPTY, toBuffer(prefix), sep])

    return new Wrapper(this.bee, {
      autoClose: false,
      prefix,
      sep: this.sep,
      keyEncoding: opts.keyEncoding ? codecs(opts.keyEncoding) : this._unprefixedKeyEncoding,
      valueEncoding: opts.valueEncoding ? codecs(opts.valueEncoding) : this.valueEncoding
    })
  }

  snapshot() {
    return this._remap(this.bee.snapshot())
  }

  checkout(opts) {
    return this._remap(this.bee.checkout(opts))
  }

  undo(n) {
    return this._remap(this.bee.undo(n))
  }

  async get(key, opts) {
    const encoding = this._getEncoding(opts)
    const entry = await this.bee.get(enc(encoding.key, key), opts)
    return final(entry, encoding)
  }

  async peek(range, opts) {
    const encoding = this._getEncoding(opts)
    const entry = await this.bee.peek(this._encRange(encoding.key, { ...opts, ...range }))
    return final(entry, encoding)
  }

  download(range, opts) {
    const encoding = this._getEncoding(opts)
    return this.bee.download(this._encRange(encoding.key, { ...opts, ...range }))
  }

  createReadStream(range, opts) {
    opts = opts ? { ...opts, ...range } : range

    const encoding = this._getEncoding(opts)
    const stream = this.bee.createReadStream(this._encRange(encoding.key, { ...opts, ...range }))

    return pipeline(
      stream,
      new Transform({
        transform(entry, cb) {
          cb(null, final(entry, encoding))
        }
      })
    )
  }

  createDiffStream(right, range, opts) {
    if (right instanceof Wrapper) right = right.bee

    // backwards compat range arg
    opts = opts ? { ...opts, ...range } : range

    const encoding = this._getEncoding(opts)
    const stream = this.bee.createDiffStream(
      right,
      this._encRange(encoding.key, { ...opts, ...range })
    )

    return pipeline(
      stream,
      new Transform({
        transform(diff, cb) {
          cb(null, {
            left: final(diff.left, encoding),
            right: final(diff.right, encoding)
          })
        }
      })
    )
  }

  write(opts) {
    return new WriteBatch(this.bee.write(opts), this._getEncoding(opts))
  }

  put(key, value, opts) {
    const batch = this.write(opts)
    batch.tryPut(key, value)
    return batch.flush()
  }

  del(key, opts) {
    const batch = this.write(opts)
    batch.tryDelete(key)
    return batch.flush()
  }

  _remap(bee) {
    return new Wrapper(bee, {
      prefix: this.prefix,
      sep: this.sep,
      keyEncoding: this._unprefixedKeyEncoding,
      valueEncoding: this.valueEncoding
    })
  }

  _getEncoding(opts) {
    if (!opts || (!opts.keyEncoding && !opts.valueEncoding)) {
      return { key: this.keyEncoding, value: this.valueEncoding }
    }

    let key = this.keyEncoding
    if (opts.keyEncoding) {
      key = codecs(opts.keyEncoding)
      if (this.prefix) key = prefixEncoding(this.prefix, key)
    }

    return {
      key,
      value: opts.valueEncoding ? codecs(opts.valueEncoding) : this.valueEncoding
    }
  }

  _encRange(keyEncoding, opts) {
    return encRange(keyEncoding, { ...opts, sub: this._sub })
  }
}

function encRange(e, opts) {
  if (e && e.encodeRange) {
    const r = e.encodeRange({ gt: opts.gt, gte: opts.gte, lt: opts.lt, lte: opts.lte })
    opts.gt = r.gt
    opts.gte = r.gte
    opts.lt = r.lt
    opts.lte = r.lte
    return opts
  }

  if (opts.gt !== undefined) opts.gt = enc(e, opts.gt)
  if (opts.gte !== undefined) opts.gte = enc(e, opts.gte)
  if (opts.lt !== undefined) opts.lt = enc(e, opts.lt)
  if (opts.lte !== undefined) opts.lte = enc(e, opts.lte)

  // a sub without explicit bounds is bounded to its own prefix
  if (opts.sub && !opts.gt && !opts.gte) opts.gt = enc(e, SEP)
  if (opts.sub && !opts.lt && !opts.lte) {
    const lt = bump(enc(e, EMPTY))
    if (lt !== null) opts.lt = lt
  }

  return opts
}

function bump(key) {
  // key was copied by enc above, safe to mutate
  for (let i = key.length - 1; i >= 0; i--) {
    if (key[i] !== 0xff) {
      key[i]++
      return key.subarray(0, i + 1)
    }
  }
  return null
}

function enc(e, v) {
  if (v === undefined || v === null) return null
  if (e !== null) return e.encode(v)
  if (typeof v === 'string') return b4a.from(v)
  return v
}

function prefixEncoding(prefix, keyEncoding) {
  return {
    encode(key) {
      return b4a.concat([prefix, b4a.isBuffer(key) ? key : enc(keyEncoding, key)])
    },
    decode(key) {
      const sliced = key.subarray(prefix.length, key.length)
      return keyEncoding ? keyEncoding.decode(sliced) : sliced
    }
  }
}

function final(entry, encoding) {
  if (!entry) return null
  return {
    ...entry,
    key: encoding.key ? encoding.key.decode(entry.key) : entry.key,
    value:
      entry.value === null || entry.value === undefined || encoding.value === null
        ? entry.value
        : encoding.value.decode(entry.value)
  }
}

function toBuffer(v) {
  return b4a.isBuffer(v) ? v : b4a.from(v)
}

module.exports = Wrapper
