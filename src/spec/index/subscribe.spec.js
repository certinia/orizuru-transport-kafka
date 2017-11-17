/**
 * Copyright (c) 2017, FinancialForce.com, inc
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 *   are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *      this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *      this list of conditions and the following disclaimer in the documentation 
 *      and/or other materials provided with the distribution.
 * - Neither the name of the FinancialForce.com, inc nor the names of its contributors 
 *      may be used to endorse or promote products derived from this software without 
 *      specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES 
 *  OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL 
 *  THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 *  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 *  OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 *  OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

'use strict';

const

	FIRST_INVOCATION = 0,
	FIRST_ARGUMENT = 0,
	FIRST_STRATEGY = 0,

	Kafka = require('no-kafka'),

	sinon = require('sinon'),
	proxyquire = require('proxyquire'),

	{ calledOnce, notCalled, calledWith } = sinon.assert,

	sandbox = sinon.sandbox.create(),
	restore = sandbox.restore.bind(sandbox),

	chaiAsPromised = require('chai-as-promised'),
	anyFunction = sinon.match.func,

	chai = require('chai'),
	expect = chai.expect;

chai.use(chaiAsPromised);

describe('index/subscribe.js', () => {

	describe('subscribe', () => {

		let subscribe, mocks, handler, config, topic;

		beforeEach(() => {

			mocks = {};
			mocks.kafka = {
				GroupConsumer: sandbox.stub()
			};

			mocks.kafka.GroupConsumer.prototype.init = sandbox.stub().resolves();

			subscribe = proxyquire('../../lib/index/subscribe', {
				'no-kafka': mocks.kafka
			});

			handler = sandbox.stub();

			config = {};
			topic = 'test';
		});

		afterEach(restore);

		it('creates a consumer and initialises it', () => {

			// given 

			config = {
				connectionString: 'server.com:9092'
			};

			// when - then

			return expect(subscribe.subscribe({ eventName: topic, handler, config })).to.be.fulfilled
				.then(() => {

					calledOnce(mocks.kafka.GroupConsumer);
					calledWith(mocks.kafka.GroupConsumer, {
						connectionString: 'server.com:9092',
						groupId: topic,
						startingOffset: Kafka.EARLIEST_OFFSET
					});
					calledOnce(mocks.kafka.GroupConsumer.prototype.init);
					calledWith(mocks.kafka.GroupConsumer.prototype.init, [{
						subscriptions: [topic],
						handler: anyFunction
					}]);

				});

		});

		it('rejects if GroupConsumer constructor throws an error', () => {

			// given

			mocks.kafka.GroupConsumer.throws(new Error('Constructor error'));

			// when - then

			return expect(subscribe.subscribe({ eventName: 'test', handler, config }))
				.to.be.rejectedWith('Constructor error');

		});

		it('rejects if GroupConsumer init throws an error', () => {

			// given

			mocks.kafka.GroupConsumer.prototype.init.throws(new Error('Init error'));

			// when - then

			return expect(subscribe.subscribe({ eventName: 'test', handler, config }))
				.to.be.rejectedWith('Init error');

		});

		it('rejects if handler is not a function', () => {

			// given

			handler = {};

			// when - then

			return expect(subscribe.subscribe({ eventName: topic, handler, config }))
				.to.be.rejectedWith('Handler is not a Function.');

		});

	});

	describe('handler', () => {

		let subscribe, mocks, getHandlerRegisteredWithKafka,
			messageSet, topic, partition, config, handler;

		beforeEach(() => {

			mocks = {};
			mocks.kafka = {
				GroupConsumer: sandbox.stub()
			};

			mocks.kafka.GroupConsumer.prototype.init = sandbox.stub().resolves();
			mocks.kafka.GroupConsumer.prototype.commitOffset = sandbox.stub().resolves();

			subscribe = proxyquire('../../lib/index/subscribe', {
				'no-kafka': mocks.kafka
			});

			getHandlerRegisteredWithKafka = () => {
				return mocks.kafka.GroupConsumer.prototype.init
					.args[FIRST_INVOCATION][FIRST_ARGUMENT][FIRST_STRATEGY].handler;
			};

			messageSet = [{
				offset: 99,
				message: {
					value: Buffer.from('Test')
				}
			}];
			topic = 'test';
			partition = 0;
			config = {};

		});

		afterEach(restore);

		it('handler calls underlying one and commits offset', () => {

			// given

			handler = sandbox.stub().resolves();

			// when

			return subscribe.subscribe({ eventName: topic, handler, config })
				.then(() => {

					// when - then

					return expect(getHandlerRegisteredWithKafka()(messageSet, topic, partition)).to.be.fulfilled
						.then(() => {

							calledOnce(handler);
							calledWith(handler, messageSet[0].message.value);

							calledOnce(mocks.kafka.GroupConsumer.prototype.commitOffset);
							calledWith(mocks.kafka.GroupConsumer.prototype.commitOffset, {
								topic: topic,
								partition: partition,
								offset: 99
							});

						});

				});

		});

		it('underlying handler throws then offset is not committed', () => {

			// given

			handler = sandbox.stub().throws(new Error('Handler error'));

			return subscribe.subscribe({ eventName: topic, handler, config })
				.then(() => {

					// when - then

					return expect(getHandlerRegisteredWithKafka()(messageSet, topic, partition)).to.be.rejectedWith('Handler error')
						.then(() => {

							calledOnce(handler);
							calledWith(handler, messageSet[0].message.value);

							notCalled(mocks.kafka.GroupConsumer.prototype.commitOffset);

						});

				});

		});

		it('underlying handler rejects then offset is not committed', () => {

			// given

			handler = sandbox.stub().rejects(new Error('Handler error'));

			return subscribe.subscribe({ eventName: topic, handler, config })
				.then(() => {

					// when - then

					return expect(getHandlerRegisteredWithKafka()(messageSet, topic, partition)).to.be.rejectedWith('Handler error')
						.then(() => {

							calledOnce(handler);
							calledWith(handler, messageSet[0].message.value);

							notCalled(mocks.kafka.GroupConsumer.prototype.commitOffset);

						});

				});

		});

		it('rejects if commits offsets rejects', () => {

			// given

			handler = sandbox.stub().resolves();
			mocks.kafka.GroupConsumer.prototype.commitOffset.rejects(new Error('Can\'t commit'));

			return expect(subscribe.subscribe({ eventName: topic, handler, config })).to.be.fulfilled
				.then(() => {

					// when - then

					return expect(getHandlerRegisteredWithKafka()(messageSet, topic, partition))
						.to.be.rejectedWith('Can\'t commit');

				});

		});

	});

});
