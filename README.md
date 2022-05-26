# Why's Principled File Server

An alternative ipfs daemon.

## Why?

This isn't a full reimplementation, but a principled repackaging of the
underlying components that make up an ipfs node, using the learnings I gained
from building and running estuary. A number of pain points, default settings,
and performance issues have been improved, including:
- Fast reference count based pinning
- Significantly improved default bitswap parameters
- Default usage of the accelerated DHT client 
- Default flatfs parameters work well at large scales on good filesystems (ext4, xfs, probably others) 
- More to come...

## API

The api is a simple REST api interface with pretty straightforward semantics
that should feel familiar to users of estuary.

TODO: document the api

## License
MIT/Apache2 permissive stack
