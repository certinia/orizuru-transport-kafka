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
	sinon = require('sinon'),
	{ calledOnce, calledTwice, calledWith } = sinon.assert,
	proxyquire = require('proxyquire'),

	sandbox = sinon.sandbox.create(),
	restore = sandbox.restore.bind(sandbox);

describe('index/publish.js', () => {

	describe('publish', () => {

		let publish, mocks;

		beforeEach(() => {
			mocks = {};
			mocks.kafka = {
				Producer: sandbox.stub()
			};
			mocks.producer = {
				send: sandbox.stub()
			};
			mocks.kafka.Producer.prototype.init = sandbox.stub().resolves(mocks.producer);

			publish = proxyquire('../../lib/index/publish', {
				'no-kafka': mocks.kafka
			});
		});

		afterEach(restore);

		it('sends a message to a kafka partition', () => {

			// given

			const
				result = [{ topic: 'kafka-test-topic', partition: 0, offset: 353 }],
				input = {
					eventName: 'com.ffdc.Test',
					buffer: Buffer.from('Hello World'),
					config: {}
				};

			mocks.producer.send.resolves(result);

			// when

			return publish.publish(input)
				.then(() => {

					// then

					calledOnce(mocks.kafka.Producer.prototype.init);
					calledOnce(mocks.producer.send);
					calledWith(mocks.producer.send, {
						topic: input.eventName,
						message: {
							value: input.buffer
						}
					});

				});

		});

		it('reuses the producer', () => {

			// given

			const
				result = [{ topic: 'kafka-test-topic', partition: 0, offset: 353 }],
				input = {
					eventName: 'com.ffdc.Test',
					buffer: Buffer.from('Hello World'),
					config: {}
				};

			mocks.producer.send.resolves(result);

			// when

			return publish.publish(input)
				.then(() => publish.publish(input))
				.then(() => {

					// then

					calledOnce(mocks.kafka.Producer.prototype.init);
					calledTwice(mocks.producer.send);

				});

		});

	});

});
