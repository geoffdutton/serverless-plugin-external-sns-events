'use strict';
var _ = require('underscore'),
    Class = require('class.extend'),
    DELIVERY_POLICY_ATTR = 'DeliveryPolicy',
    INVOKE_URL_TEMPLATE = 'https://%s.execute-api.%s.amazonaws.com/%s/%s',
    util = require('util'),
    DEFAULT_REGION = 'us-east-1',
    BpPromise = require('bluebird');

module.exports = Class.extend({

   init: function(serverless, opts) {
      this._serverless = serverless;
      this._opts = opts;
      this.provider = serverless ? serverless.getProvider('aws') : null;

      this.hooks = {
         'info:info': this._loopEvents.bind(this, this.getInfo),
         'deploy:compileEvents': this._loopEvents.bind(this, this.addEventPermission),
         'deploy:deploy': this._loopEvents.bind(this, this.subscribeFunction),
         'before:remove:remove': this._loopEvents.bind(this, this.unsubscribeFunction),
         'subscribeExternalSNS:subscribe': this._loopEvents.bind(this, this.subscribeFunction),
         'unsubscribeExternalSNS:unsubscribe': this._loopEvents.bind(this, this.unsubscribeFunction),
      };

      this.commands = {
         subscribeExternalSNS: {
            usage: 'Adds subscriptions to any SNS Topics defined by externalSNS.',
            lifecycleEvents: [ 'subscribe' ],
         },
         unsubscribeExternalSNS: {
            usage: 'Removes subscriptions to any SNS Topics defined by externalSNS.',
            lifecycleEvents: [ 'unsubscribe' ],
         },
      };
   },

   _loopEvents: function(fn) {
      var self = this;

      _.each(this._serverless.service.functions, function(fnDef, fnName) {
         _.each(fnDef.events, function(evt) {
            if (evt.externalSNS) {

               fn.call(self, fnName, fnDef, evt.externalSNS);
            }
         });
      });
   },

   addEventPermission: function(fnName, fnDef, topicName) {
      var normalizedTopicName, permRef,
          normalizedFnName = this._normalize(fnName),
          fnRef = normalizedFnName + 'LambdaFunction',
          permission;

      if (_.isObject(topicName)) {
         topicName = topicName.topic;
      }

      normalizedTopicName = this._normalizeTopicName(topicName);
      permRef = normalizedFnName + 'LambdaPermission' + normalizedTopicName;

      permission = {
         Type: 'AWS::Lambda::Permission',
         Properties: {
            FunctionName: { 'Fn::GetAtt': [ fnRef, 'Arn' ] },
            Action: 'lambda:InvokeFunction',
            Principal: 'sns.amazonaws.com',
            SourceArn: { 'Fn::Join': [ ':', [ 'arn:aws:sns', { 'Ref': 'AWS::Region' }, { 'Ref': 'AWS::AccountId' }, topicName ] ] }
         },
      };

      this._serverless.service.provider.compiledCloudFormationTemplate.Resources[permRef] = permission;
   },

   subscribeFunction: function(fnName, fnDef, topicName) {
      var self = this,
          subscriptionAttrs, endpoint, params,
          protocol = 'lambda';

      if (_.isObject(topicName)) {
         if (_.has(topicName, DELIVERY_POLICY_ATTR)) {
            subscriptionAttrs = {
               name: DELIVERY_POLICY_ATTR,
               value: topicName[DELIVERY_POLICY_ATTR]
            };
         }

         if (topicName.Protocol) {
            protocol = topicName.Protocol;
         }

         topicName = topicName.topic;
      }

      if (this._opts.noDeploy) {
         this._serverless.cli.log(
            'Not subscribing ' + fnDef.name + ' to ' + topicName + ' because of the noDeploy flag'
         );
         return;
      }

      this._serverless.cli.log('Need to subscribe ' + fnDef.name + ' to ' + topicName);

      return self._getFunctionEndpoint(fnDef)
          .then(function(fnEndpoint) {
             endpoint = fnEndpoint;
             return self._getSubscriptionInfo(fnDef, topicName, protocol, fnEndpoint || null);
          })
         .then(function(info) {
            endpoint = endpoint || info.FunctionArn;

            if (info.SubscriptionArn) {
               self._serverless.cli.log('Function ' + info.FunctionArn + ' is already subscribed to ' + info.TopicArn);
               if (subscriptionAttrs && info.SubscriptionArn) {
                  self._serverless.cli.log('Setting subscription attributes');
                  return self._setSubscriptionAttributes(info.SubscriptionArn, subscriptionAttrs);
               }
               return;
            }

            params = {
               TopicArn: info.TopicArn,
               Protocol: protocol,
               Endpoint: endpoint
            };
            return self.provider.request('SNS', 'subscribe', params, self._opts.stage, self._opts.region)
               .then(function(response) {
                  self._serverless.cli.log('Function ' + info.FunctionArn + ' is now subscribed to ' + info.TopicArn);
                  if (subscriptionAttrs && response.SubscriptionArn) {
                     if (response.SubscriptionArn.toLowerCase().indexOf('pending confirmation') !== -1) {
                        self._serverless.cli.log('Subscription is pending, resubscribing, retrying in 10 seconds');
                        return BpPromise.delay(10000).then(function() {
                           return self._getSubscriptionInfo.bind(self)(fnDef, topicName, protocol);
                        })
                            .then(function(newInfo) {
                               self._serverless.cli.log('Setting subscription attributes on: ' + newInfo.SubscriptionArn);
                               return self._setSubscriptionAttributes(newInfo.SubscriptionArn, subscriptionAttrs);
                            });
                     }
                     self._serverless.cli.log('Setting subscription attributes');
                     return self._setSubscriptionAttributes(response.SubscriptionArn, subscriptionAttrs);
                  }
                  return;
               });
         });
   },

   unsubscribeFunction: function(fnName, fnDef, topicName) {
      var self = this;

      if (_.isObject(topicName)) {
         topicName = topicName.topic;
      }
      this._serverless.cli.log('Need to unsubscribe ' + fnDef.name + ' from ' + topicName);

      return this._getSubscriptionInfo(fnDef, topicName)
         .then(function(info) {
            var params = { SubscriptionArn: info.SubscriptionArn };

            if (!info.SubscriptionArn) {
               self._serverless.cli.log('Function ' + info.FunctionArn + ' is not subscribed to ' + info.TopicArn);
               return;
            }

            return self.provider.request('SNS', 'unsubscribe', params, self._opts.stage, self._opts.region)
               .then(function() {
                  self._serverless.cli.log(
                     'Function ' + info.FunctionArn + ' is no longer subscribed to ' + info.TopicArn +
                     ' (deleted ' + info.SubscriptionArn + ')'
                  );
                  return;
               });
         });
   },

   _getSubscriptionInfo: function(fnDef, topicName, protocol, httpEndpoint) {
      var self = this,
          params = { FunctionName: fnDef.name },
          fnArn, acctID, region, topicArn;

      return this.provider.request('Lambda', 'getFunction', params, this._opts.stage, this._opts.region)
         .then(function(fn) {
            fnArn = fn.Configuration.FunctionArn;
            // NOTE: assumes that the topic is in the same account and region at this point
            region = fnArn.split(':')[3];
            acctID = fnArn.split(':')[4];
            topicArn = 'arn:aws:sns:' + region + ':' + acctID + ':' + topicName;

            self._serverless.cli.log('Function ARN: ' + fnArn);
            self._serverless.cli.log('Topic ARN: ' + topicArn);

            // NOTE: does not support NextToken and paginating through subscriptions at this point
            return self.provider.request('SNS', 'listSubscriptionsByTopic',
               { TopicArn: topicArn }, self._opts.stage, self._opts.region
            );

         })
         .then(function(resp) {
            var existing;

            existing = _.find(resp.Subscriptions, function(sub) {
               if (sub.SubscriptionArn.toLowerCase().indexOf('pending') !== -1) {
                  return false;
               }
               return sub.Protocol === protocol || 'lambda' &&
                   (sub.Endpoint === httpEndpoint || sub.Endpoint === fnArn);
            }) || {};

            return {
               FunctionArn: fnArn,
               TopicArn: topicArn,
               SubscriptionArn: existing.SubscriptionArn,
               Endpoint: existing.Endpoint,
            };
         });
   },

   _getRestApiId: function() {
      var self = this,
          apiName = self.provider.naming.getApiGatewayName();

      return this.provider.request('APIGateway', 'getRestApis', {}, this._opts.stage, this._opts.region)
          .then(function(res) {
             return _.find(res.items, function(item) {
                return apiName === item.name;
             });
          })
          .then(function(api) {
             return api ? api.id : null;
          });
   },

   _getFunctionEndpoint: function(fnDef) {
      var self = this,
          httpEv, apiPath;

      httpEv = _.find(fnDef.events, function(ev) {
         return ev.hasOwnProperty('http');
      });
      apiPath = httpEv ? httpEv.http.path : null;
      return this._getRestApiId().then(function(id) {
         if (id && apiPath) {
            return util.format(INVOKE_URL_TEMPLATE, id, self._opts.region || DEFAULT_REGION, self._opts.stage, apiPath);
         }
         return null;
      });
   },

   _normalize: function(s) {
      if (_.isEmpty(s)) {
         return;
      }

      return s[0].toUpperCase() + s.substr(1);
   },

   _normalizeTopicName: function(arn) {
      return this._normalize(arn.replace(/[^0-9A-Za-z]/g, ''));
   },

   _setSubscriptionAttributes: function(subscriptionArn, attribute) {
      var self = this,
          value, params;

      if (!subscriptionArn) {
         return BpPromise.resolve('Subscription ARN was empty');
      }
      value = {
         healthyRetryPolicy: attribute.value
      };

      params = {
         AttributeName: attribute.name,
         SubscriptionArn: subscriptionArn,
         AttributeValue: JSON.stringify(value)
      };

      return self.provider.request('SNS', 'setSubscriptionAttributes', params, self._opts.stage, self._opts.region);
   },

});
