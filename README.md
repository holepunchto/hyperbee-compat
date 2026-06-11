# hyperbee-compat

Legacy compat wrapper for hyperbee2

## Usage

```js
const HyperbeeCompat = require('hyperbee-compat')
const Hyperbee2 = require('hyperbee2')

const bee = new Hyperbee2(store)
const compat = new HyperbeeCompat(bee)

const entries = compat.createReadStream(indexEncoder)
```

## API

#### `compat = new HyperbeeCompat(bee, opts)`

#### `compat.ready()`

#### `compat.close()`

#### `compat.head()`

#### `compat.update(root)`

#### `compat.replicate(opts)`

#### `compat.sub(prefix, opts)`

#### `compat.snapshot()`

#### `compat.checkout(opts)`

#### `compat.undo(n)`

#### `compat.get(key, opts)`

#### `compat.peek(range, opts)`

#### `compat.download(range, opts)`

#### `compat.createReadStream(range, opts)`

#### `compat.createDiffStream(right, range, opts)`

#### `compat.put(key, value, opts)`

#### `compat.del(key, opts)`

#### `compat.write(opts)`

#### `batch.tryPut(key, value)`

#### `batch.tryDelete(key)`

#### `batch.tryClear()`

#### `batch.lock()`

#### `batch.flush()`

#### `batch.close()`

## License

Apache-2.0
