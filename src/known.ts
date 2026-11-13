/**
 * What a device last settled with the hub, per entity, kept by the host from
 * one session to the next (SyncManagerOptions.knownStore).
 *
 * Without it, every start builds a fresh engine and seeds every local entity
 * stamped now: a device that was closed then sends its old copies as the
 * newest writes, so edits made meanwhile on other devices are overwritten and
 * deletions made meanwhile come back. With it, an entity whose data is what
 * was settled is seeded with that write's stamp and not sent, so the next pull
 * brings it up to date; only what changed since is sent as new.
 */
export interface KnownEntry {
	type: string;
	key: string;
	/** The settled write's stamp, in HLC.pack form. */
	hlc: string;
	/** fingerprint(JSON.stringify(data)); '' for a deletion. */
	hash: string;
	deleted: boolean;
}

export interface KnownStore {
	/** Every entry kept for this account; none when the entries were kept for
	 *  another account (an entry is only good for the account it settled in). */
	load(account: string): Promise<KnownEntry[]> | KnownEntry[];
	/** Entries a sync settled, each added or replacing the one for its (type, key). */
	save(account: string, entries: KnownEntry[]): Promise<void> | void;
}

/**
 * A short fingerprint of a string (cyrb53's two 32-bit halves): enough to tell
 * whether an entity changed since it settled. Not a security boundary.
 */
export function fingerprint(s: string): string {
	let h1 = 0xdeadbeef ^ s.length;
	let h2 = 0x41c6ce57 ^ s.length;
	for (let i = 0; i < s.length; i++) {
		const ch = s.charCodeAt(i);
		h1 = Math.imul(h1 ^ ch, 2654435761);
		h2 = Math.imul(h2 ^ ch, 1597334677);
	}
	h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
	h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
	return (h2 >>> 0).toString(16).padStart(8, '0') + (h1 >>> 0).toString(16).padStart(8, '0');
}

/** A KnownStore in memory: for tests, and hosts that only need it for a page's life. */
export function memoryKnownStore(): KnownStore & { readonly entries: Map<string, KnownEntry> } {
	const entries = new Map<string, KnownEntry>();
	let kept: string | null = null;
	const id = (e: KnownEntry): string => `${e.type}\u0000${e.key}`;
	return {
		entries,
		load(account) {
			if (kept !== account) return [];
			return [...entries.values()];
		},
		save(account, list) {
			if (kept !== account) {
				entries.clear();
				kept = account;
			}
			for (const e of list) entries.set(id(e), e);
		}
	};
}
