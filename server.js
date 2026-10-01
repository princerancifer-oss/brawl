// Brawl multiplayer server. Zero dependencies: `node server.js` (PORT=8080). Put behind TLS (wss://) in production.
// Dev only: SIM_LAG=120 SIM_LOSS=0.05 node server.js  -> simulated latency (ms) / packet loss on outgoing messages.
const http=require('http'),crypto=require('crypto');
const PORT=+process.env.PORT||8080,LAG=+process.env.SIM_LAG||0,LOSS=+process.env.SIM_LOSS||0,MAXMSG=8192,RATE=150;
const rooms=new Map(),queue=[];let queueN=0;
const fs=require('fs'),path=require('path'),HTML=process.env.HTML||'Brawl_Prototype_FuwaMoco.html';
const srv=http.createServer((q,r)=>{fs.readFile(path.join(__dirname,HTML),(e,d)=>{if(e){r.writeHead(200);return r.end('brawl server ok (put '+HTML+' next to server.js to serve the game)')}r.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});r.end(d)})});
srv.on('upgrade',(req,sock)=>{const k=req.headers['sec-websocket-key'];if(!k)return sock.destroy();
 sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+crypto.createHash('sha1').update(k+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64')+'\r\n\r\n');
 const c={sock,buf:Buffer.alloc(0),name:'Player',dev:'PC',ping:0,room:null,slot:-1,tok:RATE,tt:Date.now(),alive:true};
 sock.on('data',d=>{c.buf=Buffer.concat([c.buf,d]);if(c.buf.length>MAXMSG*2)return sock.destroy();let m;while((m=frame(c))!==null){if(m===false)return sock.destroy();if(m!==undefined)onMsg(c,m)}});
 sock.on('close',()=>drop(c));sock.on('error',()=>sock.destroy())});
function frame(c){const b=c.buf;if(b.length<2)return null;const op=b[0]&15,fin=b[0]&128,mk=b[1]&128;let l=b[1]&127,o=2;if(!mk||!fin)return false;
 if(l==126){if(b.length<4)return null;l=b.readUInt16BE(2);o=4}else if(l==127)return false;if(l>MAXMSG)return false;if(b.length<o+4+l)return null;
 const key=b.slice(o,o+4),p=Buffer.from(b.slice(o+4,o+4+l));for(let i=0;i<l;i++)p[i]^=key[i&3];c.buf=b.slice(o+4+l);
 if(op==8){c.sock.end();return undefined}if(op==9||op==10)return undefined;if(op!=1)return false;return p.toString('utf8')}
function send(c,o){if(!c||!c.sock.writable||(LOSS&&Math.random()<LOSS))return;const go=()=>{try{const p=Buffer.from(JSON.stringify(o)),h=p.length<126?Buffer.from([129,p.length]):Buffer.from([129,126,p.length>>8,p.length&255]);c.sock.write(Buffer.concat([h,p]))}catch(e){}};LAG?setTimeout(go,LAG):go()}
const code=()=>{let s;do{s='';for(let i=0;i<5;i++)s+='ABCDEFGHJKLMNPQRSTUVWXYZ23456789'[crypto.randomInt(32)]}while(rooms.has(s));return s};
const isInt=(v,a,b)=>Number.isInteger(v)&&v>=a&&v<=b,clean=s=>String(s||'').replace(/[^A-Za-z0-9 _-]/g,'').trim().slice(0,12);
const other=c=>c.room&&c.room.p[1-c.slot],pub=r=>({t:'room',code:r.code,st:r.st,names:r.p.map(x=>x?x.name:null),devs:r.p.map(x=>x?x.dev:null),sel:r.sel,ready:r.rdy,stage:r.stage,rm:r.rm});
const bc=r=>r.p.forEach((x,i)=>send(x,{...pub(r),slot:i}));
function mkRoom(h,sc){const r={code:code(),st:'WAITING',p:[h,null],sel:[0,0],rdy:[0,0],stage:-1,sc:isInt(sc,1,16)?sc:3,rm:[0,0],tm:null};rooms.set(r.code,r);h.room=r;h.slot=0;return r}
function join(r,g){r.p[1]=g;g.room=r;g.slot=1;r.st='LOBBY';r.rdy=[0,0];bc(r)}
function drop(c){c.alive=false;const qi=queue.indexOf(c);if(qi>=0)queue.splice(qi,1);const r=c.room;if(!r)return;const o=r.p[1-c.slot];
 if(o){send(o,{t:'peer_left',inmatch:r.st=='IN_MATCH'||r.st=='STARTING'});o.room=null;o.slot=-1}r.st='CLOSED';clearTimeout(r.tm);rooms.delete(r.code);c.room=null}
function onMsg(c,raw){const n=Date.now();c.tok=Math.min(RATE,c.tok+(n-c.tt)*RATE/1000);c.tt=n;if(--c.tok<0)return;let m;try{m=JSON.parse(raw)}catch(e){return}if(!m||typeof m.t!='string')return;const r=c.room,t=m.t;
 if(t=='ping'){if(isInt(m.p,0,9999))c.ping=m.p;const q=other(c);return send(c,{t:'pong',ts:+m.ts||0,op:q?q.ping:-1,od:q?q.dev:null})}
 if(t=='hello'){c.name=clean(m.name)||'Player';c.dev=['PC','MOBILE','TABLET'].includes(m.dev)?m.dev:'PC';return send(c,{t:'hi',name:c.name})}
 if(t=='create'&&!r){const x=mkRoom(c,m.sc);return send(c,{t:'created',code:x.code}),bc(x)}
 if(t=='join'&&!r){const x=rooms.get(String(m.code||'').toUpperCase().slice(0,5));if(!x)return send(c,{t:'err',m:'Room not found.'});if(x.p[1]||x.st!='WAITING')return send(c,{t:'err',m:'Room is full.'});return join(x,c)}
 if(t=='quick'&&!r){if(queue.includes(c))return;const o=queue.shift();if(o&&o.alive){const x=mkRoom(o,m.sc);join(x,c)}else queue.push(c);return send(c,{t:'searching'})}
 if(t=='cancel'){const i=queue.indexOf(c);if(i>=0)queue.splice(i,1);if(r&&r.st=='WAITING')drop(c);return}
 if(t=='leave'){if(r)drop(c);return}
 if(!r)return;const hs=c.slot==0,o=other(c);
 if(t=='pick'&&r.st=='LOBBY'&&isInt(m.c,0,63)){r.sel[c.slot]=m.c;r.rdy[c.slot]=0;return bc(r)}
 if(t=='stage'&&r.st=='LOBBY'&&hs&&isInt(m.s,-1,r.sc-1)){r.stage=m.s;r.rdy=[0,0];return bc(r)}
 if(t=='ready'&&r.st=='LOBBY'&&o){r.rdy[c.slot]=m.r?1:0;bc(r);if(r.rdy[0]&&r.rdy[1]){r.st='STARTING';const s=r.stage<0?crypto.randomInt(r.sc):r.stage;bc(r);r.p.forEach(x=>send(x,{t:'starting',c:r.sel,stage:s}));r.tm=setTimeout(()=>{if(r.st=='STARTING'){r.st='IN_MATCH';r.p.forEach(x=>send(x,{t:'go'}))}},3200)}return}
 if(t=='inp'&&r.st=='IN_MATCH'&&!hs&&Array.isArray(m.i)&&m.i.length==12&&m.i.every(v=>isInt(v,-1,1)))return send(o,{t:'inp',i:m.i});
 if(t=='snap'&&r.st=='IN_MATCH'&&hs&&m.d&&typeof m.d=='object'&&raw.length<MAXMSG)return send(o,{t:'snap',d:m.d});
 if(t=='result'&&r.st=='IN_MATCH'&&hs&&isInt(m.w,0,1)&&Array.isArray(m.s)&&m.s.length==2&&m.s.every(v=>isInt(v,0,99))&&Array.isArray(m.g)&&m.g.length==2&&m.g.every(v=>Number.isFinite(v)&&v>=0&&v<=999)){r.st='RESULTS';r.rm=[0,0];const d={t:'result',w:m.w,s:m.s,g:m.g,names:r.p.map(x=>x.name),sel:r.sel};r.p.forEach(x=>send(x,d));return bc(r)}
 if(t=='rematch'&&r.st=='RESULTS'&&o){r.rm[c.slot]=1;bc(r);if(r.rm[0]&&r.rm[1]){r.st='LOBBY';r.rdy=[0,0];r.rm=[0,0];r.p.forEach(x=>send(x,{t:'rematch_go'}));bc(r)}return}
 if(t=='say'&&o&&isInt(m.i,0,3))return send(o,{t:'say',i:m.i})}
srv.listen(PORT,()=>console.log('brawl server listening on :'+PORT));
if(require.main!==module)module.exports=srv;
