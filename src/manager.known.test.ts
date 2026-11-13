import { describe, it, expect } from 'vitest';
import { SyncManager } from './manager.js';
import { memorySessionStore } from './session-store.js';
import { generateMEK, importMasterKey } from './crypto.js';
import { memoryKnownStore, type KnownStore } from './known.js';
import { SyncEngine, type OutgoingItem, type RemoteItem, type SyncTransport } from './engine.js';

/**
 * 0.4.0: a start that keeps what was last settled with the hub. Before it,
 * every start seeded every local entity stamped now, so a device that had been
 * closed sent its old copies as the newest writes: an edit made meanwhile on
 * another device was overwritten, and a deletion made meanwhile came back.
 */

/** A hub that stores the latest blob per item id, as the real one does. */
function itemHub() {
	let items: Array<{ id: string; seq: number; blob: string }> = [];
	let seq = 0;
	const json = (status: number, body: unknown) =>
		new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
	const fetchFn = (async (input: RequestInfo | URL, init?: RequestInit) => {
		const url = new URL(String(input), 'https://hub.test');
		const method = init?.method ?? 'GET';
		if (url.pathname === '/api/sync/items' && method === 'POST') {
			const body = JSON.parse(String(init?.body)) as { items: Array<{ id: string; blob: string }> };
			const acks = body.items.map((it) => {
				seq += 1;
				items = [...items.filter((x) => x.id !== it.id), { id: it.id, seq, blob: it.blob }];
				return { id: it.id, seq };
			});
			return json(200, { acks });
		}
		if (url.pathname === '/api/sync/items') {
			const since = Number(url.searchParams.get('since') ?? '0');
			return json(200, { items: items.filter((it) => it.seq > since).sort((a, b) => a.seq - b.seq) });
		}
		return json(404, { message: 'no such route' });
	}) as typeof fetch;
	return {
		fetchFn,
		get count() {
			return items.length;
		},
		/** The hub loses everything (a restore from an old backup, say). */
		wipe() {
			items = [];
		}
	};
}

/** A store of { key → data }, kept outside any manager, as a device's library is. */
function library(initial: Record<string, unknown>) {
	let value: Record<string, unknown> = { ...initial };
	const subs = new Set<(v: Record<string, unknown>) => void>();
	const store = {
		subscribe(fn: (v: Record<string, unknown>) => void) {
			subs.add(fn);
			fn(value);
			return () => subs.delete(fn);
		},
		update(fn: (v: Record<string, unknown>) => Record<string, unknown>) {
			value = fn(value);
			for (const s of subs) s(value);
		}
	};
	const descriptor = {
		type: 'shelf',
		toItems: (v: Record<string, unknown>) => Object.entries(v).map(([key, data]) => ({ key, data })),
		applyItem: (v: Record<string, unknown>, c: { key: string; data: unknown; deleted: boolean }) => {
			const next = { ...v };
			if (c.deleted) delete next[c.key];
			else next[c.key] = c.data;
			return next;
		}
	};
	return {
		store,
		descriptor,
		get value() {
			return value;
		},
		set(key: string, data: unknown) {
			store.update((v) => ({ ...v, [key]: data }));
		},
		remove(key: string) {
			store.update((v) => {
				const next = { ...v };
				delete next[key];
				return next;
			});
		}
	};
}

/** One device: its library and known store outlive each run of the app. */
function device(hub: ReturnType<typeof itemHub>, mek: Uint8Array, id: string, initial: Record<string, unknown>, known: KnownStore | undefined) {
	const lib = library(initial);
	let m: SyncManager | null = null;
	const open = async () => {
		const session = memorySessionStore();
		await session.save({ token: `tok-${id}`, email: 'Reader@Example.test', masterKey: await importMasterKey(mek) });
		m = new SyncManager({ apiBase: 'https://hub.test', deviceId: id, sessionStore: session, fetchFn: hub.fetchFn, knownStore: known });
		m.register(lib.descriptor, lib.store);
		await m.init();
	};
	return {
		lib,
		open,
		sync: () => m!.sync(),
		/** The app closes: nothing runs until it opens again. */
		close() {
			m?.signOut();
			m = null;
		}
	};
}

async function twoDevices(known: boolean) {
	const hub = itemHub();
	const mek = generateMEK();
	const phone = device(hub, mek, 'phone', { reading: { name: 'Reading' } }, known ? memoryKnownStore() : undefined);
	const web = device(hub, mek, 'web', {}, known ? memoryKnownStore() : undefined);
	await phone.open();
	await web.open();
	await phone.sync();
	expect(web.lib.value).toEqual({ reading: { name: 'Reading' } });
	return { hub, mek, phone, web };
}

describe('SyncManager with a known store — a device that was closed starts without undoing others', () => {
	it('takes an edit made meanwhile instead of overwriting it', async () => {
		const { phone, web } = await twoDevices(true);
		phone.close();
		web.lib.set('reading', { name: 'Reading now' });
		await web.sync();
		await phone.open();
		expect(phone.lib.value).toEqual({ reading: { name: 'Reading now' } });
		await web.sync();
		expect(web.lib.value).toEqual({ reading: { name: 'Reading now' } });
	});

	it('takes a deletion made meanwhile instead of bringing the entity back', async () => {
		const { phone, web } = await twoDevices(true);
		phone.close();
		web.lib.remove('reading');
		await web.sync();
		await phone.open();
		expect(phone.lib.value).toEqual({});
		await web.sync();
		expect(web.lib.value).toEqual({});
	});

	it('sends what changed while it was closed, as new', async () => {
		const { phone, web } = await twoDevices(true);
		phone.close();
		phone.lib.set('reading', { name: 'Renamed while away' });
		phone.lib.set('later', { name: 'Later' });
		await phone.open();
		await web.sync();
		expect(web.lib.value).toEqual({ reading: { name: 'Renamed while away' }, later: { name: 'Later' } });
	});

	it("sends again what the hub doesn't have any more", async () => {
		const { hub, mek, phone } = await twoDevices(true);
		phone.close();
		hub.wipe();
		await phone.open();
		expect(hub.count).toBe(1);
		const fresh = device(hub, mek, 'fresh', {}, memoryKnownStore());
		await fresh.open();
		expect(fresh.lib.value).toEqual({ reading: { name: 'Reading' } });
	});

	it('only uses what was kept for the same account', async () => {
		const known = memoryKnownStore();
		known.save('reader@example.test', [{ type: 'shelf', key: 'reading', hlc: '000000000000001:000000:x', hash: 'h', deleted: false }]);
		expect(await known.load('someone@else.test')).toEqual([]);
		expect((await known.load('reader@example.test')).length).toBe(1);
	});
});

describe('SyncManager without a known store — as before 0.4.0', () => {
	it('sends everything as new at each start', async () => {
		const { phone, web } = await twoDevices(false);
		phone.close();
		web.lib.set('reading', { name: 'Reading now' });
		await web.sync();
		await phone.open();
		await web.sync();
		// The old copy went out as the newest write: what 0.4.0 lets a host avoid.
		expect(web.lib.value).toEqual({ reading: { name: 'Reading' } });
	});
});

describe('SyncEngine — a write made while a push is on its way', () => {
	it('is not taken as sent when the earlier write is acknowledged', async () => {
		let release: () => void = () => {};
		const gate = new Promise<void>((r) => (release = r));
		const rows = new Map<string, { seq: number; blob: Uint8Array }>();
		let seq = 0;
		const pushes: number[] = [];
		const transport: SyncTransport = {
			async pull(since: number): Promise<RemoteItem[]> {
				return [...rows.entries()].filter(([, r]) => r.seq > since).map(([id, r]) => ({ id, seq: r.seq, blob: r.blob }));
			},
			async push(items: OutgoingItem[]) {
				pushes.push(items.length);
				if (pushes.length === 1) await gate; // the first push is slow
				return items.map(({ id, blob }) => {
					rows.set(id, { seq: ++seq, blob });
					return { id, seq };
				});
			}
		};
		const engine = await SyncEngine.create(generateMEK(), transport);
		engine.set('shelf', 'reading', { name: 'one' });
		const first = engine.sync();
		await new Promise((r) => setTimeout(r, 10));
		engine.set('shelf', 'reading', { name: 'two' }); // while 'one' is on its way
		release();
		await first;
		await engine.sync();
		// Before 0.4.0 the ack for 'one' marked 'two' as sent: the second sync sent nothing.
		expect(pushes).toEqual([1, 1]);
	});
});
