#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015, 2016, 2017 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
import json

from backtrader import BrokerBase, OrderBase, Order
from backtrader.position import Position
from backtrader.utils.py3 import queue, with_metaclass

from .ccxtstore import CCXTStore
from datetime import datetime
from threading import Thread, Event
import time
# from multiprocessing.pool import ThreadPool as Pool
import sys
import datetime as dt


class CCXTOrder(OrderBase):
    def __init__(self, owner, data, ccxt_order):
        self.owner = owner
        self.data = data
        self.ccxt_order = ccxt_order
        self.executed_fills = []
        self.ordtype = self.Buy if ccxt_order['side'] == 'buy' else self.Sell
        self.size = float(ccxt_order['amount'])

        super(CCXTOrder, self).__init__()


class MetaCCXTBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaCCXTBroker, cls).__init__(name, bases, dct)
        CCXTStore.BrokerCls = cls


class CCXTBroker(with_metaclass(MetaCCXTBroker, BrokerBase)):
    '''Broker implementation for CCXT cryptocurrency trading library.
    This class maps the orders/positions from CCXT to the
    internal API of ``backtrader``.

    Broker mapping added as I noticed that there differences between the expected
    order_types and retuned status's from canceling an order

    Added a new mappings parameter to the script with defaults.

    Added a get_balance function. Manually check the account balance and update brokers
    self.cash and self.value. This helps alleviate rate limit issues.

    Added a new get_wallet_balance method. This will allow manual checking of the any coins
        The method will allow setting parameters. Useful for dealing with multiple assets

    Modified getcash() and getvalue():
        Backtrader will call getcash and getvalue before and after next, slowing things down
        with rest calls. As such, th

    The broker mapping should contain a new dict for order_types and mappings like below:

    broker_mapping = {
        'order_types': {
            bt.Order.Market: 'market',
            bt.Order.Limit: 'limit',
            bt.Order.Stop: 'stop-loss', #stop-loss for kraken, stop for bitmex
            bt.Order.StopLimit: 'stop limit'
        },
        'mappings':{
            'closed_order':{
                'key': 'status',
                'value':'closed'
                },
            'canceled_order':{
                'key': 'result',
                'value':1}
                }
        }

    Added new private_end_point method to allow using any private non-unified end point

    '''

    order_types = {Order.Market: 'market',
                   Order.Limit: 'limit',
                   Order.Stop: 'stop',  # stop-loss for kraken, stop for bitmex
                   Order.StopLimit: 'stop limit'}

    mappings = {
        'closed_order': {
            'key': 'status',
            'value': 'closed'
        },
        'canceled_order': {
            'key': 'status',
            'value': 'canceled'}
    }

    def __init__(self, broker_mapping=None, debug=False, **kwargs):
        super(CCXTBroker, self).__init__()

        if broker_mapping is not None:
            try:
                self.order_types = broker_mapping['order_types']
            except KeyError:  # Might not want to change the order types
                pass
            try:
                self.mappings = broker_mapping['mappings']
            except KeyError:  # might not want to change the mappings
                pass

        self.store = CCXTStore(**kwargs)

        self.currency = self.store.currency

        self.positions = collections.defaultdict(Position)

        self.debug = debug
        self.indent = 4  # For pretty printing dictionaries

        self.notifs = queue.Queue()  # holds orders which are notified

        self.open_orders = list()

        self.startingcash = self.store._cash
        self.startingvalue = self.store._value

        t_order_updates = Thread(target=self.fetch_order_updates, daemon=True)
        t_order_updates.start()

    def _fetch_order_updates(self, o_order):
        # TODO - Remove cancelled order from array
        # TODO - Kill this thread after run
        try:
            oID = o_order.ccxt_order['id']

            # Print debug before fetching so we know which order is giving an
            # issue if it crashes
            if self.debug:
                print('Fetching Order ID: {}'.format(oID))

            # Get the order
            ccxt_order = self.store.fetch_order(oID, o_order.data.p.dataname)
            print('ccxt_order - ', ccxt_order)
            # Check for new fills
            if 'trades' in ccxt_order and ccxt_order['trades'] != None:
                for fill in ccxt_order['trades']:
                    if fill not in o_order.executed_fills:
                        o_order.execute(fill['datetime'], fill['amount'], fill['price'],
                                        0, 0.0, 0.0,
                                        0, 0.0, 0.0,
                                        0.0, 0.0,
                                        0, 0.0)
                        o_order.executed_fills.append(fill['id'])

            if self.debug:
                print(json.dumps(ccxt_order, indent=self.indent))

            # Check if the order is closed
            if ccxt_order[self.mappings['closed_order']['key']] == self.mappings['closed_order']['value']:
                pos = self.getposition(o_order.data, clone=False)
                pos.update(o_order.size, o_order.price)
                o_order.completed()
                self.notify(o_order)
                self.open_orders.remove(o_order)
                self.get_balance()
            if ccxt_order[self.mappings['canceled_order']['key']] == self.mappings['canceled_order']['value']:
                pos = self.getposition(o_order.data, clone=False)
                pos.update(o_order.size, o_order.price)
                o_order.cancel()
                self.notify(o_order)
                self.open_orders.remove(o_order)
                self.get_balance()
                # print(self.open_orders)
        except Exception as e:
            print("ERROR: {}".format(sys.exc_info()[0]))
            print("{}".format(e))

        return o_order

    def fetch_order_updates(self):
        # Make individual calls to each fetch open order, parellel
        # Run once every 30 seconds
        while (True):
            if dt.datetime.now().second / 30 == 1:
                if len(self.open_orders) > 0:
                    for open_order in list(self.open_orders):
                        t_fetch_order_updates = Thread(target=self._fetch_order_updates, args=[open_order])
                        t_fetch_order_updates.start()
                # So that it doesn't run a lot within that 1 second
                time.sleep(2)

    def get_balance(self):
        self.store.get_balance()
        self.cash = self.store._cash
        self.value = self.store._value
        return self.cash, self.value

    def get_wallet_balance(self, currency, params={}):
        balance = self.store.get_wallet_balance(currency, params=params)
        try:
            cash = balance['free'][currency] if balance['free'][currency] else 0
        except KeyError:  # never funded or eg. all USD exchanged
            cash = 0
        try:
            value = balance['total'][currency] if balance['total'][currency] else 0
        except KeyError:  # never funded or eg. all USD exchanged
            value = 0
        return cash, value

    def getcash(self):
        # Get cash seems to always be called before get value
        # Therefore it makes sense to add getbalance here.
        # return self.store.getcash(self.currency)
        self.cash = self.store._cash
        return self.cash

    def getvalue(self, datas=None):
        # return self.store.getvalue(self.currency)
        self.value = self.store._value
        return self.value

    def get_notification(self):
        try:
            return self.notifs.get(False)
        except queue.Empty:
            return None

    def notify(self, order):
        self.notifs.put(order)

    def getposition(self, data, clone=True):
        # return self.o.getposition(data._dataname, clone=clone)
        pos = self.positions[data._dataname]
        if clone:
            pos = pos.clone()
        return pos

    def next(self):
        if self.debug:
            print('Broker next() called')
        # Put this in a thread and call it once every 5 seconds

    def _submit(self, owner, data, execType, side, amount, price, params):

        order_type = self.order_types.get(execType) if execType else 'market'
        created = int(data.datetime.datetime(0).timestamp()*1000)
        # Extract CCXT specific params if passed to the order
        params = params['params'] if 'params' in params else params
        if order_type == 'STOP_MARKET':
            params['stopPrice'] = price
        # Binance doesn't want price for market orders
        if order_type == 'market':
            price = None
        print('amount')
        print(amount)
        print('price')
        print(price)
        print('order_type')
        print(order_type)
        print('side')
        print(side)
        print('params')
        print(params)
        # params['created'] = created  # Add timestamp of order creation for backtesting
        print(datetime.now())
        ret_ord = self.store.create_order(symbol=data.p.dataname, order_type=order_type, side=side,
                                          amount=amount, price=price, params=params)
        print(ret_ord)
        _order = self.store.fetch_order(ret_ord['id'], data.p.dataname)

        order = CCXTOrder(owner, data, _order)
        order.price = ret_ord['price']
        self.open_orders.append(order)

        self.notify(order)
        return order

    def submit_batch_order(self, orders):
        # params['created'] = created  # Add timestamp of order creation for backtesting
        # print(datetime.now())
        to_return_orders = []
        try:
            returned_orders, owner_data_list = self.store.create_batch_order(orders)
            for returned_order in returned_orders:
                _order = self.store.fetch_order(returned_order['orderId'], returned_order['symbol'].split('USDT')[0] + '/USDT')
                for owner_data in owner_data_list:
                    if returned_order['symbol'] == owner_data['symbol']:
                        owner = owner_data['owner']
                        data = owner_data['data']
                        order = CCXTOrder(owner, data, _order)
                        to_return_orders.append(order)
                        order.price = returned_order['price']
                        self.open_orders.append(order)
                        self.notify(order)
            return to_return_orders
        except Exception as e:
            print("ERROR: {}".format(sys.exc_info()[0]))
            print("{}".format(e))


        # print(ret_ord)
        # [{'orderId': 10260663906, 'symbol': 'BTCUSDT', 'status': 'NEW', 'clientOrderId': 'iWm3n8yYouGS5tAxzvpmIB0',
        #   'price': '9511.51', 'avgPrice': '0.00000', 'origQty': '0.004', 'executedQty': '0', 'cumQty': '0',
        #   'cumQuote': '0', 'timeInForce': 'GTC', 'type': 'LIMIT', 'reduceOnly': False, 'closePosition': False,
        #   'side': 'BUY', 'positionSide': 'BOTH', 'stopPrice': '0', 'workingType': 'CONTRACT_PRICE',
        #   'priceProtect': False, 'origType': 'LIMIT', 'updateTime': 1607906881443},
        #  {'orderId': 8389765489693389308, 'symbol': 'ETHUSDT', 'status': 'NEW',
        #   'clientOrderId': 'bmMXu5uagFrB5ZDawLxqvt1', 'price': '290.68', 'avgPrice': '0.00000', 'origQty': '0.159',
        #   'executedQty': '0', 'cumQty': '0', 'cumQuote': '0', 'timeInForce': 'GTC', 'type': 'LIMIT',
        #   'reduceOnly': False, 'closePosition': False, 'side': 'BUY', 'positionSide': 'BOTH', 'stopPrice': '0',
        #   'workingType': 'CONTRACT_PRICE', 'priceProtect': False, 'origType': 'LIMIT', 'updateTime': 1607906881443}]
        # From single order
        # {'info': {'orderId': 10260745391, 'symbol': 'BTCUSDT', 'status': 'NEW',
        #           'clientOrderId': 'A0C9wx3W75ASM00XzMpU5p', 'price': '0', 'avgPrice': '0.00000', 'origQty': '0.004',
        #           'executedQty': '0', 'cumQty': '0', 'cumQuote': '0', 'timeInForce': 'GTC', 'type': 'MARKET',
        #           'reduceOnly': False, 'closePosition': False, 'side': 'BUY', 'positionSide': 'BOTH', 'stopPrice': '0',
        #           'workingType': 'CONTRACT_PRICE', 'priceProtect': False, 'origType': 'MARKET',
        #           'updateTime': 1607907002333}, 'id': '10260745391', 'clientOrderId': 'A0C9wx3W75ASM00XzMpU5p',
        #  'timestamp': None, 'datetime': None, 'lastTradeTimestamp': None, 'symbol': 'BTC/USDT', 'type': 'market',
        #  'side': 'buy', 'price': 0.0, 'amount': 0.004, 'cost': 0.0, 'average': None, 'filled': 0.0, 'remaining': 0.004,
        #  'status': 'open', 'fee': None, 'trades': None}

    def buy(self, owner, data, size, price=None, plimit=None,
            execType=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):
        del kwargs['parent']
        del kwargs['transmit']
        return self._submit(owner, data, execType, 'buy', size, price, kwargs)

    def sell(self, owner, data, size, price=None, plimit=None,
             execType=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             **kwargs):
        del kwargs['parent']
        del kwargs['transmit']
        return self._submit(owner, data, execType, 'sell', size, price, kwargs)

    def cancel(self, order):

        oID = order.ccxt_order['id']

        if self.debug:
            print('Broker cancel() called')
            print('Fetching Order ID: {}'.format(oID))

        # check first if the order has already been filled otherwise an error
        # might be raised if we try to cancel an order that is not open.
        ccxt_order = self.store.fetch_order(oID, order.data.p.dataname)

        if self.debug:
            print(json.dumps(ccxt_order, indent=self.indent))
            # print('closed_order, key')
            # print(ccxt_order[self.mappings['closed_order']['key']])
            # print('closed_order, value')
            # print(ccxt_order[self.mappings['closed_order']['value']])
            # print('closed_order')
            # print(ccxt_order[self.mappings['closed_order']])

        if ccxt_order[self.mappings['closed_order']['key']] == self.mappings['closed_order']['value']:
            return order

        ccxt_order = self.store.cancel_order(oID, order.data.p.dataname)

        if self.debug:
            print(json.dumps(ccxt_order, indent=self.indent))
            # print('canceled_order')
            # print(ccxt_order[self.mappings['canceled_order']])
            # print('canceled_order, key')
            # print(ccxt_order[self.mappings['canceled_order']['key']])
            # print('canceled_order, value')
            # print(ccxt_order[self.mappings['canceled_order']['value']])
            # Commenting things that are breaking the code but the order is still cancelled.
            # print('Value Received: {}'.format(ccxt_order[self.mappings['canceled_order']['key']]))
            print('Value Expected: {}'.format(self.mappings['canceled_order']['value']))

        # if ccxt_order[self.mappings['canceled_order']['key']] == self.mappings['canceled_order']['value']:
        self.open_orders.remove(order)
        order.cancel()
        self.notify(order)
        return order

    def get_orders_open(self, safe=False):
        return self.store.fetch_open_orders()

    def private_end_point(self, type, endpoint, params):
        '''
        Open method to allow calls to be made to any private end point.
        See here: https://github.com/ccxt/ccxt/wiki/Manual#implicit-api-methods

        - type: String, 'Get', 'Post','Put' or 'Delete'.
        - endpoint = String containing the endpoint address eg. 'order/{id}/cancel'
        - Params: Dict: An implicit method takes a dictionary of parameters, sends
          the request to the exchange and returns an exchange-specific JSON
          result from the API as is, unparsed.

        To get a list of all available methods with an exchange instance,
        including implicit methods and unified methods you can simply do the
        following:

        print(dir(ccxt.hitbtc()))
        '''
        endpoint_str = endpoint.replace('/', '_')
        endpoint_str = endpoint_str.replace('{', '')
        endpoint_str = endpoint_str.replace('}', '')

        method_str = 'private_' + type.lower() + endpoint_str.lower()

        return self.store.private_end_point(type=type, endpoint=method_str, params=params)
