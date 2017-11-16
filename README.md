# Orizuru Transport Redis.

[![Build Status](https://travis-ci.org/financialforcedev/orizuru-transport-redis.svg?branch=master)](https://travis-ci.org/financialforcedev/orizuru-transport-redis)
[![NSP Status](https://nodesecurity.io/orgs/ffres/projects/a598bbf3-fdde-4ee0-9b4a-17d6e579512a/badge)](https://nodesecurity.io/orgs/ffres/projects/a598bbf3-fdde-4ee0-9b4a-17d6e579512a)

Orizuru Transport Redis is a transport library for the [Orizuru](https://www.npmjs.com/package/@financialforcedev/orizuru) framework.

## Install

```
$ npm install @financialforcedev/orizuru-transport-redis
```

## Usage

Use this dependency to specify the transport layer that ```@financialforcedev/orizuru``` uses as Redis.

	const
		// get classes from orizuru
		{ Server, Handler, Publisher } = require('@financialforcedev/orizuru'),

		// get the transport
		transport = require('@financialforcedev/orizuru-transport-redis'),

		// configure the transport
		transportConfig = {
			url: 'redis://localhost:6379'
		};

	new Server({ transport, transportConfig }))...
	new Handler({ transport, transportConfig })...
	new Publisher({ transport, transportConfig })...


## API Docs

Click to view [JSDoc API documentation](http://htmlpreview.github.io/?https://github.com/financialforcedev/orizuru-transport-redis/blob/master/doc/index.html).