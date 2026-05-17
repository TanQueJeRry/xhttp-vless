import { connect } from 'cloudflare:sockets';

export default {
  async fetch(request, env) {
    const uuid = (env.UUID || '').toLowerCase().replace(/-/g, '');
    if (request.method !== 'POST' || !request.body) {
      return new Response('{"status":"ok"}', {
        headers: { 'Content-Type': 'application/json' }
      });
    }
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    handleVless(request.body, writer, uuid).catch(() => {
      try { writer.close(); } catch {}
    });
    return new Response(readable, {
      headers: { 'Content-Type': 'application/octet-stream' }
    });
  }
};

async function handleVless(body, writer, uuid) {
  const reader = body.getReader();
  let buf = new Uint8Array(0);
  let ready = false;
  let rw = null;

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      const tmp = new Uint8Array(buf.length + value.length);
      tmp.set(buf); tmp.set(value, buf.length); buf = tmp;

      if (!ready) {
        const h = parseVless(buf, uuid);
        if (!h) continue;
        ready = true;
        await writer.write(new Uint8Array([0, 0]));
        const sock = connect({ hostname: h.host, port: h.port });
        rw = sock.writable.getWriter();
        pipe(sock.readable, writer);
        if (h.data.length) await rw.write(h.data);
        buf = new Uint8Array(0);
      } else {
        if (rw) await rw.write(value);
      }
    }
  } finally {
    try { rw && await rw.close(); } catch {}
    try { writer.close(); } catch {}
  }
}

async function pipe(readable, writer) {
  const reader = readable.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      await writer.write(value);
    }
  } catch {}
  try { writer.close(); } catch {}
}

function parseVless(data, uuid) {
  if (data.length < 22 || data[0] !== 0) return null;
  const id = [...data.slice(1,17)].map(b=>b.toString(16).padStart(2,'0')).join('');
  if (id !== uuid) return null;
  let i = 18 + data[17];
  if (data.length < i + 4) return null;
  i++; // skip cmd
  const port = (data[i] << 8) | data[i+1]; i += 2;
  const at = data[i++];
  let host = '';
  if (at === 1) { host = [...data.slice(i,i+4)].join('.'); i+=4; }
  else if (at === 2) { const l=data[i++]; host=new TextDecoder().decode(data.slice(i,i+l)); i+=l; }
  else if (at === 3) {
    const p=[];
    for(let j=0;j<16;j+=2) p.push(((data[i+j]<<8)|data[i+j+1]).toString(16));
    host=p.join(':'); i+=16;
  }
  return { host, port, data: data.slice(i) };
}
