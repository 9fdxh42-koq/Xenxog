import { Boom } from '@hapi/boom';
import NodeCache from 'node-cache';
import readline from 'readline';
import makeWASocket, {
	delay,
	proto,
	AnyMessageContent,
	DisconnectReason,
	fetchLatestBaileysVersion,
	getAggregateVotesInPollMessage,
	makeCacheableSignalKeyStore,
	makeInMemoryStore,
	PHONENUMBER_MCC,
	useMultiFileAuthState,
	WAMessageContent,
	WAMessageKey,
	WAMessageStubType,
	Browsers,
	getContentType,
	jidNormalizedUser
} from '../src';

import MAIN_LOGGER from '../src/Utils/logger';
import open from 'open';
import fs from 'fs';
import { format } from "util";

const logger = MAIN_LOGGER.child({});
//logger.level = 'trace';

const str2Regex = (str: string) => str.replace(/[|\\{}()[\]^$+*?.]/g, '\\$&');
const prefix = new RegExp('^([' + ('‎/!#$%+£¢€¥^°=¶∆×÷π√✓©®:;?&.\\-').replace(/[|\\{}()[\]^$+*?.\-\^]/g, '\\$&') + '])');

const useStore = !process.argv.includes('--no-store');
const doReplies = !process.argv.includes('--no-reply');
const usePairingCode = process.argv.includes('--use-pairing-code');
const useMobile = process.argv.includes('--mobile');

// external map to store retry counts of messages when decryption/encryption fails
// keep this out of the socket itself, so as to prevent a message decryption/encryption loop across socket restarts
const msgRetryCounterCache = new NodeCache();

// Read line interface
const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
const question = (text: string) => new Promise<string>((resolve) => rl.question(text, resolve));

// the store maintains the data of the WA connection in memory
// can be written out to a file & read from it
const store = useStore ? makeInMemoryStore({ logger }) : undefined;
store?.readFromFile('./baileys_store_multi.json');
// save every 10s
setInterval(() => {
	store?.writeToFile('./baileys_store_multi.json')
}, 10_000);

function patchMessageBeforeSending(msg: proto.IMessage, jid: string[]): Promise<proto.IMessage> | proto.IMessage {
    //console.log({ jid, msg: JSON.stringify(msg, null, 2) });
    if (msg?.deviceSentMessage?.message?.listMessage) {
        //msg.deviceSentMessage.message.listMessage.listType = proto.Message.ListMessage.ListType.SINGLE_SELECT;
        console.log("ListType in deviceSentMessage is patched:", msg.deviceSentMessage.message.listMessage.listType);
    };
    
    if (msg?.listMessage) {
        //msg.listMessage.listType = proto.Message.ListMessage.ListType.SINGLE_SELECT;
        console.log("ListType in listMessage is patched:", msg.listMessage.listType);
    };
    
    const requiresPatch = !!(msg.buttonsMessage || msg.templateMessage || msg.listMessage);
    if (requiresPatch) {
        msg = {
            viewOnceMessage: {
                message: {
                    messageContextInfo: {
                        deviceListMetadata: {},
                        deviceListMetadataVersion: 2
                    },
                    ...msg
                }
            }
        };
    };
    
    console.log(JSON.stringify(msg, null, 2));
    return msg;
};

// start a connection
const startSock = async() => {
	const { state, saveCreds } = await useMultiFileAuthState('baileys_auth_info');
	// fetch latest version of WA Web
	const { version, isLatest } = await fetchLatestBaileysVersion();
	console.log(`using WA v${version.join('.')}, isLatest: ${isLatest}`);
	const browser = Browsers.macOS("Safari");
	
	const sock = makeWASocket({
		version,
		logger,
		browser,
		printQRInTerminal: !usePairingCode,
		mobile: useMobile,
		auth: {
			creds: state.creds,
			/** caching makes the store faster to send/recv messages */
			keys: makeCacheableSignalKeyStore(state.keys, logger),
		},
		syncFullHistory: false,
		msgRetryCounterCache,
		generateHighQualityLinkPreview: true,
		// ignore all broadcast messages -- to receive the same
		// comment the line below out
		// shouldIgnoreJid: jid => isJidBroadcast(jid),
		// implement to handle retries & poll updates
		getMessage,
		patchMessageBeforeSending
	});
	
	store?.bind(sock.ev);
	
	// Pairing code for Web clients
	if(usePairingCode && !sock.authState.creds.registered) {
		if(useMobile) {
			throw new Error('Cannot use pairing code with mobile api');
		};
		
		const phoneNumber = await question('Please enter your mobile phone number:\n');
		const code = await sock.requestPairingCode(phoneNumber);
		console.log(`Pairing code for '${phoneNumber}': ${code?.match(/.{1,4}/g)?.join('-') || code}`);
	};
	
	const reply = async (jid: string, msg: AnyMessageContent, options: object) => {
		await sock.presenceSubscribe(jid);
		await delay(500);
		
		await sock.sendPresenceUpdate('composing', jid);
		await delay(2000);
		
		await sock.sendPresenceUpdate('paused', jid);
		
		await sock.sendMessage(jid, msg, options);
	};
	
	// the process function lets you process all events that just occurred
	// efficiently in a batch
	sock.ev.process(
		// events is a map for event name => event data
		async(events) => {
			// something about the connection changed
			// maybe it closed, or we received all offline message or connection opened
			if(events['connection.update']) {
				const update = events['connection.update']
				const { connection, lastDisconnect } = update
				const code = (lastDisconnect?.error as Boom)?.output?.statusCode || (lastDisconnect?.error as Boom)?.output?.payload?.statusCode;
				
				if(code) console.log({ code, reason: DisconnectReason[code] });
				
				if(connection === 'close') {
					// reconnect if not logged out
					if (code !== DisconnectReason.loggedOut) {
						startSock();
					} else {
						console.log('Connection closed. You are logged out.')
					}
				}
				
				console.log('connection update', update);
			};
			
			// credentials updated -- save them
			if(events['creds.update']) {
				await saveCreds();
			};
			
			if(events.call) {
				console.log('recv call event', events.call);
			};
			
			// messages updated like status delivered, message deleted etc.
			if(events['messages.update']) {
				//console.log(
				//	JSON.stringify(events['messages.update'], undefined, 2)
				//)
				
				for(const { key, update } of events['messages.update']) {
					if(update.pollUpdates) {
						const pollCreation = await getMessage(key)
						if(pollCreation) {
							console.log(
								'got poll update, aggregation: ',
								getAggregateVotesInPollMessage({
									message: pollCreation,
									pollUpdates: update.pollUpdates,
								})
							)
						}
					}
				}
			};
			
			// received a new message
			if(events['messages.upsert']) {
				const upsert = events['messages.upsert'];
				let m = upsert.messages[upsert.messages.length - 1];
				m = proto.WebMessageInfo.fromObject(m);
				const senderKeyDistributionMessage = m.message?.senderKeyDistributionMessage?.groupId;
				const chat = jidNormalizedUser(m.key?.remoteJid || (senderKeyDistributionMessage !== "status@broadcast" && senderKeyDistributionMessage) || '');
				const mtype = m.message && getContentType(m.message) || m.message && Object.keys(m.message)[0] || '';
				const msg = !m.message ? null : /viewOnceMessage/.test(mtype) ? m.message[Object.keys(m.message)[0]] : m.message[mtype];
				const body = typeof msg === "string" ? msg : msg && 'text' in msg && msg.text ? msg.text : msg && 'caption' in msg && msg.caption ? msg.caption : msg && 'contentText' in msg && msg.contentText ? msg.contentText : '';
				if (m.messageStubType) {
					console.log({
						messageStubType: WAMessageStubType[m.messageStubType],
						messageStubParameters: m.messageStubParameters,
						participant: m.participant
					})
				};
				const customPrefix = /^×?> /;
				const match = (customPrefix.test(body) ? [[customPrefix.exec(body), customPrefix]].find(p => p[1]) : [[prefix.exec(body), prefix]].find(p => p[1])) || '';
				const usedPrefix = (match[0] || match[1] || '')[0] || '';
				const noPrefix = body.replace(usedPrefix, '');
				let [command, ...args] = noPrefix.trim().split` `.filter(v => v);
				args = args || [];
				let _args = noPrefix.trim().split` `.slice(1);
				let text = _args.join` `;
				command = (command || '').toLowerCase();
				if (!usedPrefix) return;
				console.log(`[Message]: ${m.pushName} > ${usedPrefix + command}`);
				switch (command) {
					case 'list':
					    await sock.sendMessage(chat, {
						    text: 'Hello World',
						    footer: 'footer',
						    buttonText: "PILIH SATU",
						    sections: [{
							    title: "section title",
							    rows: [{
								    title: "Ping",
								    rowId: usedPrefix + "ping"
								},
								{
									title: "Menu",
									rowId: usedPrefix + "menu"
								}]
							}]
						}, { quoted: m });
					break;
					case 'ping':
						await reply(chat, { text: 'ok' }, { quoted: m })
						break;
					default:
						if (customPrefix.test(body)) {
							let i = 15;
							let _return;
							let _syntax;
							let _text = (/^(×>)/.test(usedPrefix) ? 'return ' : '') + noPrefix;
							try {
								// @ts-ignore
								let exec = new (async () => { }).constructor('print', 'm', 'sock', 'chat', 'process', 'args', 'require', _text);
								_return = await exec.call(sock, (...args) => {
									if (--i < 1) return;
									return reply(chat, { text: format(...args) }, { quoted: m });
								}, m, sock, chat, process, args, require);
							} catch (e) {
								_return = e;
							} finally {
								await sock.sendMessage(chat, { text: format(_return) }, { quoted: m });
							};
						};
				}
			};
			
		}
	);
	
	return sock;
	
	async function getMessage(key: WAMessageKey): Promise<WAMessageContent | undefined> {
		if(store) {
			const msg = await store.loadMessage(key.remoteJid!, key.id!);
			return msg?.message || undefined;
		}
		
		// only if store is present
		return proto.Message.fromObject({})
	}
};

startSock();