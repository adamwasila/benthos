---
title: encode
type: processor
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the contents of:
     lib/processor/encode.go
-->


Encodes messages according to the selected scheme.


import Tabs from '@theme/Tabs';

<Tabs defaultValue="common" values={[
  { label: 'Common', value: 'common', },
  { label: 'Advanced', value: 'advanced', },
]}>

import TabItem from '@theme/TabItem';

<TabItem value="common">

```yaml
encode:
  scheme: base64
```

</TabItem>
<TabItem value="advanced">

```yaml
encode:
  scheme: base64
  parts: []
```

</TabItem>
</Tabs>

## Fields

### `scheme`

`string` The decoding scheme to use.

Options are: `hex`, `base64`.

### `parts`

`array` An optional array of message indexes of a batch that the processor should apply to.
If left empty all messages are processed. This field is only applicable when
batching messages [at the input level](/docs/configuration/batching).

Indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1.

