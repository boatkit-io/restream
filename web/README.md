# @boatkit-io/restream

TypeScript helpers for ReStream web clients.

```bash
pnpm add @boatkit-io/restream
```

```ts
import { subscriptionKeyFromFieldPath } from '@boatkit-io/restream';

const key = subscriptionKeyFromFieldPath(['Board', 0]);
```

CommonJS consumers can use the same package entrypoint:

```js
const { subscriptionKeyFromFieldPath } = require('@boatkit-io/restream');
```
