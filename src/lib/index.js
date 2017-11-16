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
/**
 * The Index file for project.
 * Returns the publish and subscribe methods.
 * 
 * These define the API for a transport layer.
 * In this case, the transport uses redis pub/sub.
 * 
 * @module index
 */

const
	publish = require('./index/publish'),
	subscribe = require('./index/subscribe'),

	publishFunc = config => publish.publish(config),
	subscribeFunc = config => subscribe.subscribe(config);

publishFunc.emitter = publish.emitter;
subscribeFunc.emitter = subscribe.emitter;

module.exports = {
	/**
	 * @func
	 * Publish
	 * 
	 * @example
	 * let buffer = ...,
	 * 	config = {
	 * 		url: 'redis://localhost:6379'
	 * 	}
	 * index.publish({ eventName: 'test', buffer, config });
	 * 
	 * @param {object} message - { eventName, buffer, config }
	 * @param {string} message.eventName - the name of the event to publish
	 * @param {buffer} message.buffer - the message to send as a buffer
	 * @param {object} message.config - config object
	 * @returns {Promise}
	 * 
	 * @property {EventEmitter} emitter
	 * @property {string} emitter.ERROR - the error event name
	 */
	publish: publishFunc,
	/**
	 * @func
	 * Subscribe
	 * 
	 * @example
	 * let handler = (buffer) => 
	 * 	{
	 * 		console.log(buffer);
	 * 	},
	 * 	config = {
	 * 		url: 'redis://localhost:6379'
	 * 	}
	 * index.subscribe({ eventName: 'test', handler, config });
	 * 
	 * @property {EventEmitter} emitter
	 * @property {string} emitter.ERROR - the error event name
	 * 
	 * @param {object} subscriberConfig - { eventName, handler, config }
	 * @param {string} subscriberConfig.eventName - the name of the event to listen for
	 * @param {function} subscriberConfig.handler - handler function called when the event is heard
	 * @param {object} subscriberConfig.config - config object
	 * @returns {Promise}
	 */
	subscribe: subscribeFunc
};
