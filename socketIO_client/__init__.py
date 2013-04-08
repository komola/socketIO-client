#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import logging
log = logging.getLogger('socketIO_client')
import requests
import websocket
from anyjson import dumps, loads
from datetime import datetime
from threading import Thread, Event
from time import sleep, mktime


__version__ = '0.4'


PROTOCOL = 1  # SocketIO protocol version


class BaseNamespace(object):  # pragma: no cover

    def __init__(self, socketIO):
        self.socketIO = socketIO

    def on_connect(self, socketIO):
        pass

    def on_disconnect(self):
        pass

    def on_noop(self):
        pass

    def on_error(self, reason, advice):
        print '[Error] %s' % advice

    def on_message(self, messageData):
        print '[Message] %s' % messageData

    def on_(self, eventName, *eventArguments):
        print '[Event] %s%s' % (eventName, eventArguments)

    def on_open(self, *args):
        print '[Open]', args

    def on_close(self, *args):
        print '[Close]', args

    def on_retry(self, *args):
        print '[Retry]', args

    def on_reconnect(self, *args):
        print '[Reconnect]', args


def make_transport(supportedTransports, allowedTransports, secure, url, sessionID, **kwarg):
    log.debug('make_transport(%s, %s, %s, %s, %s)' % (supportedTransports,
                                                        allowedTransports,
                                                        secure,
                                                        url,
                                                        sessionID))
    transportClassMap = {
        'websocket': WebsocketTransport,
        'xhr-polling': XhrPollingTransport,
        }
    if allowedTransports is None:
        allowedTransports = ['websocket', 'xhr-polling']
    for transport in allowedTransports:
        if transport in supportedTransports and transport in transportClassMap:
            return transportClassMap[transport](secure, url, sessionID, **kwarg)
    else:
        raise SocketIOError('Could not negotiate a transport: ' \
                            'locally(%s), remote(%s)' % (allowedTransports,
                                                         supportedTransports))


class WebsocketTransport(object):
    def __init__(self, secure, url, sessionID, **kwarg):
        log.debug("WebsocketTransport(%s, %s, %s)" % (secure, url, sessionID))
        socketURL = '%s://%s/websocket/%s' % (
            'wss' if secure else 'ws', url, sessionID)
        self.connection = websocket.create_connection(socketURL)

    def send(self, packet):
        self.connection.send(packet)

    def close(self):
        self.connection.close()

    def recv(self):
        yield self.connection.recv()

    def connected(self):
        return self.connection.connected

class XhrPollingTransport(object):
    def __init__(self, secure, url, sessionID, cookies, **kwarg):
        log.debug("XhrPollingTransport(%s, %s, %s)" % (secure, url, sessionID))
        self.url = '%s://%s/xhr-polling/%s' % ('https' if secure
                else 'http', url, sessionID)
        self._connected = True
        self.session = requests.Session()
        self.session.cookies.update(cookies)

    def get_seconds_since_epoch(self):
        return int(mktime(datetime.now().timetuple()))

    def get_params(self):
        return {'t':self.get_seconds_since_epoch()}

    def send(self, packet):
        resp = self.session.post(self.url, params=self.get_params(), data=packet)
        return resp

    def close(self, reconnect=False):
        log.debug("[Disconnecting] Please wait…")
        self._connected = False
        if not reconnect:
            params = self.get_params()
            params.update({"disconnect": True})
            resp = requests.get(self.url, params=params,
                            cookies=self.session.cookies)
            return resp.text

    def connected(self):
        return self._connected

    def recv(self):
        resp = self.session.get(self.url, params=self.get_params())
        if resp.text.encode('utf-8').startswith(u'\ufffd'.encode('utf-8')):
            m = resp.text.encode('utf-8').split(u'\ufffd'.encode('utf-8'))
            for l, d in zip(m[1::2], m[2::2]):
                if len(d) != int(l):
                    log.error("Invalid incoming message: declared %d chars, receipt %d chars" % (len(d), int(l)))
                yield d
            return
        log.debug("recv(%s)" % resp.text[:15])
        yield resp.text

class SocketIO(object):

    messageID = 0

    def __init__(self, host, port, Namespace=BaseNamespace, secure=False,
                 transports=None, cookies={}, **kwarg):
        self.host = host
        self.port = int(port)
        self.namespace = Namespace(self)
        self.secure = secure
        self.transports = transports
        self.params = kwarg
        self.cookies = cookies
        self.__connect()

        heartbeatInterval = self.heartbeatTimeout - 2
        self.heartbeatThread = RhythmicThread(heartbeatInterval,
            self._send_heartbeat)
        self.heartbeatThread.start()

        self.channelByName = {}
        self.callbackByEvent = {}
        self.namespaceThread = ListenerThread(self)
        self.namespaceThread.start()

    def __del__(self):  # pragma: no cover
        self.heartbeatThread.cancel()
        self.namespaceThread.cancel()
        self.transport.close()

    def __connect(self):
        baseURL = '%s:%d/socket.io/%s' % (self.host, self.port, PROTOCOL)
        response = requests.get('%s://%s/' % ('https' if self.secure else 'http',
                                    baseURL), cookies=self.cookies)

        if 200 != response.status_code:  # pragma: no cover
            raise SocketIOError('Could not establish connection')
        responseParts = response.text.split(':')

        self.sessionID = responseParts[0]
        self.heartbeatTimeout = int(responseParts[1])
        self.connectionTimeout = int(responseParts[2])
        self.supportedTransports = responseParts[3].split(',')
        self.transport = make_transport(self.supportedTransports,
                                        self.transports, self.secure, baseURL,
                                        self.sessionID, cookies=self.cookies)
        if self.transport is None:
            raise SocketIOError('Could not parse handshake')  # pragma: no cover

    def _recv_packet(self):
        code, packetID, channelName, data = -1, None, None, None
        for packet in self.transport.recv():
            packetParts = packet.split(':', 3)
            packetCount = len(packetParts)
            if 4 == packetCount:
                code, packetID, channelName, data = packetParts
            elif 3 == packetCount:
                code, packetID, channelName = packetParts
            elif 1 == packetCount:  # pragma: no cover
                code = packetParts[0]
            yield int(code), packetID, channelName, data

    def _send_packet(self, code, channelName='', data='', callback=None):
        self.transport.send(':'.join([
            str(code),
            self.set_callback(callback) if callback else '',
            channelName,
            data]))

    def disconnect(self, channelName=''):
        self._send_packet(0, channelName)
        if channelName:
            del self.channelByName[channelName]
        else:
            self.__del__()

    @property
    def connected(self):
        return self.transport.connected()

    def connect(self, channelName, Namespace=BaseNamespace):
        channel = Channel(self, channelName, Namespace)
        self.channelByName[channelName] = channel
        self._send_packet(1, channelName)
        return channel

    def _send_heartbeat(self):
        try:
            self._send_packet(2)
        except:
            self.__del__()

    def message(self, messageData, callback=None, channelName=''):
        if isinstance(messageData, basestring):
            code = 3
            data = messageData
        else:
            code = 4
            data = dumps(messageData)
        self._send_packet(code, channelName, data, callback)

    def emit(self, eventName, *eventArguments, **eventKeywords):
        code = 5
        if callable(eventArguments[-1]):
            callback = eventArguments[-1]
            eventArguments = eventArguments[:-1]
        else:
            callback = None
        channelName = eventKeywords.get('channelName', '')
        data = dumps(dict(name=eventName, args=eventArguments))
        self._send_packet(code, channelName, data, callback)

    def get_callback(self, channelName, eventName):
        'Get callback associated with channelName and eventName'
        socketIO = self.channelByName[channelName] if channelName else self
        try:
            return socketIO.callbackByEvent[eventName]
        except KeyError:
            pass
        namespace = socketIO.namespace

        def callback_(*eventArguments):
            return namespace.on_(eventName, *eventArguments)
        return getattr(namespace, name_callback(eventName), callback_)

    def set_callback(self, callback):
        'Set callback that will be called after receiving an acknowledgment'
        self.messageID += 1
        self.namespaceThread.set_callback(self.messageID, callback)
        return '%s+' % self.messageID

    def on(self, eventName, callback):
        self.callbackByEvent[eventName] = callback

    def wait(self, seconds=None, forCallbacks=False):
        if forCallbacks:
            self.namespaceThread.wait_for_callbacks(seconds)
        elif seconds:
            sleep(seconds)
        else:
            try:
                while self.connected:
                    sleep(1)
            except KeyboardInterrupt:
                pass


class Channel(object):

    def __init__(self, socketIO, channelName, Namespace):
        self.socketIO = socketIO
        self.channelName = channelName
        self.namespace = Namespace(self)
        self.callbackByEvent = {}

    def disconnect(self):
        self.socketIO.disconnect(self.channelName)

    def emit(self, eventName, *eventArguments):
        self.socketIO.emit(eventName, *eventArguments,
            channelName=self.channelName)

    def message(self, messageData, callback=None):
        self.socketIO.message(messageData, callback,
            channelName=self.channelName)

    def on(self, eventName, eventCallback):
        self.callbackByEvent[eventName] = eventCallback


class ListenerThread(Thread):
    'Process messages from SocketIO server'

    daemon = True

    def __init__(self, socketIO):
        super(ListenerThread, self).__init__()
        self.socketIO = socketIO
        self.done = Event()
        self.waitingForCallbacks = Event()
        self.callbackByMessageID = {}
        self.get_callback = self.socketIO.get_callback

    def run(self):
        while not self.done.is_set():
            try:
                for code, packetID, channelName, data in self.socketIO._recv_packet():
                    try:
                        delegate = {
                            0: self.on_disconnect,
                            1: self.on_connect,
                            2: self.on_heartbeat,
                            3: self.on_message,
                            4: self.on_json,
                            5: self.on_event,
                            6: self.on_acknowledgment,
                            7: self.on_error,
                            8: self.on_noop,
                        }[code]
                    except KeyError:
                        continue
                    delegate(packetID, channelName, data)
            except Exception, err:
                log.exception(err)
                continue

    def cancel(self):
        self.done.set()

    def wait_for_callbacks(self, seconds):
        self.waitingForCallbacks.set()
        self.join(seconds)

    def set_callback(self, messageID, callback):
        self.callbackByMessageID[messageID] = callback

    def on_disconnect(self, packetID, channelName, data):
        callback = self.get_callback(channelName, 'disconnect')
        callback()

    def on_connect(self, packetID, channelName, data):
        callback = self.get_callback(channelName, 'connect')
        callback(self.socketIO)

    def on_heartbeat(self, packetID, channelName, data):
        pass

    def on_message(self, packetID, channelName, data):
        callback = self.get_callback(channelName, 'message')
        callback(data)

    def on_json(self, packetID, channelName, data):
        callback = self.get_callback(channelName, 'message')
        callback(loads(data))

    def on_event(self, packetID, channelName, data):
        valueByName = loads(data)
        eventName = valueByName['name']
        eventArguments = valueByName.get('args', [])
        callback = self.get_callback(channelName, eventName)
        callback(*eventArguments)

    def on_acknowledgment(self, packetID, channelName, data):
        dataParts = data.split('+', 1)
        messageID = int(dataParts[0])
        arguments = loads(dataParts[1]) or []
        try:
            callback = self.callbackByMessageID[messageID]
        except KeyError:
            pass
        else:
            del self.callbackByMessageID[messageID]
            callback(*arguments)
            callbackCount = len(self.callbackByMessageID)
            if self.waitingForCallbacks.is_set() and not callbackCount:
                self.cancel()

    def on_error(self, packetID, channelName, data):
        reason, advice = data.split('+', 1)
        callback = self.get_callback(channelName, 'error')
        callback(reason, advice)

    def on_noop(self, packetID, channelName, data):
        callback = self.get_callback(channelName, 'noop')
        callback()


class RhythmicThread(Thread):
    'Execute rhythmicFunction every few seconds'

    daemon = True

    def __init__(self, intervalInSeconds, rhythmicFunction, *args, **kw):
        super(RhythmicThread, self).__init__()
        self.intervalInSeconds = intervalInSeconds
        self.rhythmicFunction = rhythmicFunction
        self.args = args
        self.kw = kw
        self.done = Event()

    def run(self):
        try:
            while not self.done.is_set():
                self.rhythmicFunction(*self.args, **self.kw)
                self.done.wait(self.intervalInSeconds)
        except:
            pass

    def cancel(self):
        self.done.set()


class SocketIOError(Exception):
    pass


def name_callback(eventName):
    return 'on_' + eventName.replace(' ', '_')
