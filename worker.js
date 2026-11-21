import { connect } from 'cloudflare:sockets';

// Встроенный PSK-секрет (совпадает с .relay-secret на локальной машине)
const BUILTIN_RELAY_SECRET = '2c161e26e73eb630a84a3281a5016ebf9ea2ef18502fabac';

export default {
  async fetch(request, env, ctx) {
    // 1. Проверяем, что это WebSocket-апгрейд
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected WebSocket Upgrade', { status: 426 });
    }

    // 2. Проверяем PSK-секрет
    // relay.mjs передаёт секрет как WebSocket-подпротокол
    const protocolsHeader = request.headers.get('Sec-WebSocket-Protocol');
    const relaySecret = (env && env.RELAY_SECRET) ? env.RELAY_SECRET : BUILTIN_RELAY_SECRET;

    if (!relaySecret) {
      return new Response('Worker secret not configured', { status: 500 });
    }

    const clientProtocols = protocolsHeader
      ? protocolsHeader.split(',').map((p) => p.trim())
      : [];

    if (!clientProtocols.includes(relaySecret)) {
      return new Response('Unauthorized', { status: 401 });
    }

    // 3. Достаём target host и port из query
    const url = new URL(request.url);
    const targetHost = url.searchParams.get('host');
    const targetPort = parseInt(url.searchParams.get('port') || '443', 10);

    if (!targetHost || !targetPort) {
      return new Response('Missing host or port parameter', { status: 400 });
    }

    // 4. Создаём пару WebSocket'ов
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);
    server.accept();

    // 5. Открываем TCP-соединение к целевому хосту
    let tcpSocket;
    try {
      tcpSocket = connect({
        hostname: targetHost,
        port: targetPort,
      });
    } catch (err) {
      server.close(1011, `TCP Connect Failed: ${err.message}`);
      return new Response(null, { status: 101, webSocket: client });
    }

    // 6. TCP -> WebSocket
    tcpSocket.readable
      .pipeTo(
        new WritableStream({
          write(chunk) {
            server.send(chunk);
          },
          close() {
            server.close(1000, 'TCP Closed');
          },
          abort(reason) {
            server.close(1011, `TCP Aborted: ${reason}`);
          },
        })
      )
      .catch((err) => {
        console.error('TCP to WS pipe error:', err);
        try {
          server.close(1011, 'TCP Read Error');
        } catch (_) {}
      });

    // 7. WebSocket -> TCP
    server.addEventListener('message', async (event) => {
      if (!tcpSocket.writable) return;
      const writer = tcpSocket.writable.getWriter();
      try {
        const data = event.data;
        if (typeof data === 'string') {
          await writer.write(new TextEncoder().encode(data));
        } else if (data instanceof ArrayBuffer) {
          await writer.write(new Uint8Array(data));
        } else {
          await writer.write(new Uint8Array(await data.arrayBuffer()));
        }
      } catch (err) {
        console.error('WS to TCP write error:', err);
        try {
          server.close(1011, 'TCP Write Error');
        } catch (_) {}
      } finally {
        writer.releaseLock();
      }
    });

    // 8. Закрытие / ошибки
    server.addEventListener('close', () => {
      try {
        tcpSocket.close();
      } catch (_) {}
    });

    server.addEventListener('error', (err) => {
      console.error('WebSocket error:', err);
      try {
        tcpSocket.close();
      } catch (_) {}
    });

    // 9. Отдаём WebSocket-ответ
    return new Response(null, { status: 101, webSocket: client });
  },
};