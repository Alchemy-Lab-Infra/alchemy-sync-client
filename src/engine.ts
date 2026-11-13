/**
 * SyncEngine — the transport-agnostic core of E2E sync. It holds the unlocked
 * MEK, encrypts/decrypts items, and reconciles local and remote state with
 * per-item last-writer-wins keyed by a Hybrid Logical Clock.
 *
 * It talks to a `SyncTransport` (a dumb encrypted-blob store), so it can be
 * unit-tested against an in-memory transport with no server. The server only
 * ever sees `{ id, seq, blob }` — opaque ids and ciphertext.
 */
import {
	importMasterKey,
	deriveItemKeys,
	deterministicItemId,
	encryptItem,
	decryptItem,
	type ItemKeys
} from './crypto.js';
import { HLC, type HLCTimestamp } from './hlc.js';

/** Server-visible record. `seq` is a per-account monotonic counter (the sync
 *  cursor); `blob` is the encrypted item. Nothing else is exposed. */
export interface RemoteItem {
	id: string;
	seq: number;
	blob: Uint8Array;
}
export interface OutgoingItem {
	id: string;
	blob: Uint8Array;
}

export interface SyncTransport {
	/** Items with seq > sinceSeq, ascending. */
	pull(sinceSeq: number): Promise<RemoteItem[]>;
	/** Upsert; returns the server-assigned seq per id. */
	push(items: OutgoingItem[]): Promise<Array<{ id: string; seq: number }>>;
}

/**
 * Max items per push request. The server caps request size, and a first sync of
 * a large library sails past it as one request (SCP Ultimate's first push was
 * ~3,350 items ≈ 1.5 MB → HTTP 400; ≤500 worked). 200 keeps each request small
 * without chattiness, and per-batch acks mean an interrupted first sync resumes
 * where it left off instead of starting over.
 */
const PUSH_BATCH = 500; // the hub accepts up to 500 per push (limits.ts); a fresh device's first sync of a big library is ~7 batches, not 18

/** What lives inside an encrypted blob — never leaves the device in the clear. */
interface ItemPlaintext {
	type: string;
	key: string;
	data: unknown;
	hlc: HLCTimestamp;
	deleted: boolean;
}

interface LocalRecord extends ItemPlaintext {
	dirty: boolean;
}

export interface RemoteChange {
	type: string;
	key: string;
	data: unknown; // undefined when deleted
	deleted: boolean;
}

/** An entity as a sync settled it with the hub: pulled and taken, or pushed
 *  and acknowledged. */
export interface SettledRecord {
	type: string;
	key: string;
	data: unknown; // undefined when deleted
	hlc: HLCTimestamp;
	deleted: boolean;
}

export interface SyncEngineOptions {
	now?: () => number;
	/** Stable per-device id; breaks same-millisecond ties between devices. */
	node?: string;
	onRemoteChange?: (change: RemoteChange) => void;
	/**
	 * After each sync, the entities it settled with the hub, so a host can keep
	 * each one's stamp and hand it back at the next start (seedKnown).
	 */
	onSettled?: (records: SettledRecord[]) => void;
	/**
	 * Bracket the pull phase of a sync. A host that applies each remote change
	 * into a store can defer its per-change bookkeeping (snapshots, diffs) to
	 * onPullEnd, which turns a first sync of N items from O(N²) into O(N).
	 * onPullEnd runs even when the pull throws part-way.
	 */
	onPullStart?: () => void;
	onPullEnd?: () => void;
}

export class SyncEngine {
	private readonly hlc: HLC;
	/**
	 * Keyed by a local `type\0key` composite, NOT the server's opaque id. The
	 * opaque id is a keyed HMAC and computing it is async, so keying on it would
	 * force set/remove/get to be async too. Identity on pull comes from the
	 * decrypted plaintext, which also means a server that renamed ids couldn't
	 * confuse us.
	 */
	private readonly records = new Map<string, LocalRecord>();
	private cursor = 0;
	private onRemoteChange?: (change: RemoteChange) => void;
	private onPullStart?: () => void;
	private onPullEnd?: () => void;
	private onSettled?: (records: SettledRecord[]) => void;
	/** Entities seeded at their last settled stamp, until a pull from the start
	 *  has shown the hub has each of them (seedKnown). */
	private readonly seededClean = new Set<string>();

	private constructor(
		private readonly keys: ItemKeys,
		private readonly transport: SyncTransport,
		opts: SyncEngineOptions = {}
	) {
		this.hlc = new HLC(opts.now, opts.node);
		this.onRemoteChange = opts.onRemoteChange;
		this.onPullStart = opts.onPullStart;
		this.onPullEnd = opts.onPullEnd;
		this.onSettled = opts.onSettled;
	}

	/**
	 * Build an engine from either the raw MEK or an already-imported master key.
	 * Async because subkey derivation is WebCrypto; pass the CryptoKey form to
	 * keep the master key non-extractable end to end.
	 */
	static async create(
		master: Uint8Array | CryptoKey,
		transport: SyncTransport,
		opts: SyncEngineOptions = {}
	): Promise<SyncEngine> {
		const baseKey = master instanceof Uint8Array ? await importMasterKey(master) : master;
		return new SyncEngine(await deriveItemKeys(baseKey), transport, opts);
	}

	static localKey(type: string, key: string): string {
		return `${type}${String.fromCharCode(0)}${key}`;
	}

	private idFor(type: string, key: string): string {
		return SyncEngine.localKey(type, key);
	}

	/** Create or update a local entity and mark it for the next push. */
	set(type: string, key: string, data: unknown): void {
		this.records.set(this.idFor(type, key), {
			type,
			key,
			data,
			hlc: this.hlc.send(),
			deleted: false,
			dirty: true
		});
	}

	/**
	 * An entity as it was when it last settled with the hub, with that write's
	 * stamp: held, not marked to push. A start then sends nothing for an
	 * unchanged entity, so a newer write from another device (or its deletion)
	 * wins the next pull instead of losing to a copy restamped now. If the first
	 * pull from the start shows the hub doesn't have it, it is sent after all.
	 */
	seedKnown(type: string, key: string, data: unknown, hlc: HLCTimestamp): void {
		this.hlc.receive(hlc);
		const lk = this.idFor(type, key);
		this.records.set(lk, { type, key, data, hlc, deleted: false, dirty: false });
		this.seededClean.add(lk);
	}

	/** Tombstone a local entity (propagates the delete on the next push). */
	remove(type: string, key: string): void {
		this.records.set(this.idFor(type, key), {
			type,
			key,
			data: undefined,
			hlc: this.hlc.send(),
			deleted: true,
			dirty: true
		});
	}

	get(type: string, key: string): unknown {
		const r = this.records.get(this.idFor(type, key));
		return r && !r.deleted ? r.data : undefined;
	}

	/** True when a record exists for this entity, tombstones included. Lets a
	 *  caller tell "never seen" from "seen and deleted". */
	has(type: string, key: string): boolean {
		return this.records.has(this.idFor(type, key));
	}

	list(type: string): Array<{ key: string; data: unknown }> {
		return [...this.records.values()]
			.filter((r) => r.type === type && !r.deleted)
			.map((r) => ({ key: r.key, data: r.data }));
	}

	/** One reconcile cycle: pull remote changes, merge (LWW), push local changes. */
	async sync(): Promise<void> {
		const settled: SettledRecord[] = [];
		const settle = (r: ItemPlaintext): void => {
			settled.push({ type: r.type, key: r.key, data: r.deleted ? undefined : r.data, hlc: r.hlc, deleted: r.deleted });
		};
		// A pull from the start lists everything the hub has: an entity seeded as
		// settled that isn't there goes out after all (seedKnown).
		const listing = this.cursor === 0 && this.seededClean.size > 0 ? new Set<string>() : null;
		// PULL + merge. Identity comes from the decrypted plaintext, not remote.id.
		const remotes = await this.transport.pull(this.cursor);
		this.onPullStart?.();
		try {
			for (const remote of remotes) {
				this.cursor = Math.max(this.cursor, remote.seq);
				const item = await decryptItem<ItemPlaintext>(this.keys.itemKey, remote.blob);
				this.hlc.receive(item.hlc);
				const lk = SyncEngine.localKey(item.type, item.key);
				listing?.add(lk);
				const local = this.records.get(lk);
				if (!local || HLC.compare(item.hlc, local.hlc) > 0) {
					this.records.set(lk, { ...item, dirty: false });
					settle(item);
					this.onRemoteChange?.({
						type: item.type,
						key: item.key,
						data: item.deleted ? undefined : item.data,
						deleted: item.deleted
					});
				} else if (!local.dirty && HLC.compare(item.hlc, local.hlc) < 0) {
					// The hub holds an older write than one this device settled: send it again.
					local.dirty = true;
				}
			}
		} finally {
			this.onPullEnd?.();
		}
		if (listing) {
			for (const lk of this.seededClean) {
				const r = this.records.get(lk);
				if (r && !r.dirty && !listing.has(lk)) r.dirty = true;
			}
			this.seededClean.clear();
		}

		// PUSH dirty. The opaque server id is derived here (async HMAC) so the
		// synchronous set/remove path never has to await.
		const dirty = [...this.records.entries()].filter(([, r]) => r.dirty);
		if (dirty.length > 0) {
			const outgoing: OutgoingItem[] = [];
			const sentById = new Map<string, { lk: string; record: LocalRecord }>();
			for (const [lk, r] of dirty) {
				const id = await deterministicItemId(this.keys.itemIdKey, r.type, r.key);
				sentById.set(id, { lk, record: r });
				const blob = await encryptItem(this.keys.itemKey, {
					type: r.type,
					key: r.key,
					data: r.data,
					hlc: r.hlc,
					deleted: r.deleted
				} satisfies ItemPlaintext);
				outgoing.push({ id, blob });
			}
			for (let i = 0; i < outgoing.length; i += PUSH_BATCH) {
				for (const ack of await this.transport.push(outgoing.slice(i, i + PUSH_BATCH))) {
					this.cursor = Math.max(this.cursor, ack.seq);
					const sent = sentById.get(ack.id);
					// Only the write that was sent is settled: one made while it was on
					// the way replaced the record, and still has to go.
					if (sent && this.records.get(sent.lk) === sent.record) {
						sent.record.dirty = false;
						settle(sent.record);
					}
				}
			}
		}
		if (settled.length > 0) this.onSettled?.(settled);
	}
}

/**
 * In-memory transport standing in for the real encrypted-blob server. Backing
 * store is shared by reference across engines to simulate multiple devices on
 * one account. Exposes `rows` so tests can assert the server holds only opaque
 * ciphertext.
 */
export class MemoryTransport implements SyncTransport {
	readonly rows = new Map<string, { seq: number; blob: Uint8Array }>();
	private nextSeq = 1;

	async pull(sinceSeq: number): Promise<RemoteItem[]> {
		return [...this.rows.entries()]
			.filter(([, r]) => r.seq > sinceSeq)
			.sort((a, b) => a[1].seq - b[1].seq)
			.map(([id, r]) => ({ id, seq: r.seq, blob: r.blob }));
	}

	async push(items: OutgoingItem[]): Promise<Array<{ id: string; seq: number }>> {
		return items.map(({ id, blob }) => {
			const seq = this.nextSeq++;
			this.rows.set(id, { seq, blob });
			return { id, seq };
		});
	}
}
