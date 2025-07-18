const pm2 = require('pm2')
const pmx = require('pmx')
const pino = require('pino')
const { minimatch } = require('minimatch')
const { Client, Connection } = require("@opensearch-project/opensearch")

const config = pmx.initModule()

const logger = pino({
	transport: {
		target: 'pino-pretty',
		options: {
			translateTime: 'UTC:yyyy-mm-dd HH:MM:ss.l o',
		}
	}
})

const listenApps = config.include?.split(',') || []

const ignoreApps = config.exclude?.split(',') || []

logger.info({
	config,
	listenApps,
	ignoreApps,
}, 'start')

const client = new Client({
  node: config.endpoint,
	ssl: {
		rejectUnauthorized: false,
	},
	Connection,
})

function parse_data(name, raw, extra) {
	const lines = raw?.split('\n')

	return lines
		.filter(x => !!x)
		.map(x => {
			try {
				const buf = JSON.parse(x)
				return {
					...buf,
					...extra,
				}
			} catch (err) {
				logger.error({ name, raw: x, err }, 'parse_data')
				return {
					raw: x,
					...extra,
				}
			}
		})
}

function send(indexName, datasource) {
	client.helpers
	.bulk({
		datasource,
		onDocument() {
			return { index: { _index: indexName } };
		},
		onDrop(doc) {
			logger.error({ indexName, doc }, 'drop')
		}
	})
	.then((stat) => {
		if (config['show-send-stat']) {
			logger.info({ indexName, stat }, 'send');
		}
	})
	.catch(err => {
		logger.error({ indexName, err }, 'send')
	})
}

function shouldProcess(msg) {
	if (config.include === '*') {
		return !ignoreApps.some(pattern => minimatch(msg.process.name, pattern))
	} else if (config.exclude === '*') {
		return listenApps.some(pattern => minimatch(msg.process.name, pattern))
	} else {
		return listenApps.some(pattern => minimatch(msg.process.name, pattern))
			&& !ignoreApps.some(pattern => minimatch(msg.process.name, pattern))
	}
}

pm2.Client.launchBus((err, bus) => {
	if (err) return logger.error({ err }, 'bus launch')

	bus.on('log:out', (msg) => {
		if (shouldProcess(msg)) {
			const datasource = []
			const data = parse_data(msg.process.name, msg.data)
			
			if (data.length) {
				for (const item of data) {
					datasource.push({
							...item,
							time: new Date(item.time || msg.at).toISOString(),
							stream: 'stdout',
						},
					)
				}
				
				send(msg.process.name, datasource)
			}
		}
	})

	bus.on('log:err', (msg) => {
		if (shouldProcess(msg)) {
			const data = msg.data.trim()
			if (data) {
				send(msg.process.name, [{
					raw: data,
					time: new Date(msg.at),
					stream: 'stderr'
				}])
			}
		}
	})

	bus.on('reconnect attempt', () => {
		logger.info('bus reconnecting')
	})

	bus.on('close', () => {
		logger.info('bus closed')
		pm2.disconnectBus()
	})
})