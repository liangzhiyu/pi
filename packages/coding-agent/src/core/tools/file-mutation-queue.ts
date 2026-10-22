import { realpath, stat } from "node:fs/promises";
import { resolve } from "node:path";

interface FileMutationQueueEntry {
	queue: Promise<void>;
	identity: string | undefined;
}

const fileMutationQueues = new Map<string, FileMutationQueueEntry>();
let registrationQueue = Promise.resolve();

function isMissingPathError(error: unknown): boolean {
	return (
		typeof error === "object" &&
		error !== null &&
		"code" in error &&
		(error.code === "ENOENT" || error.code === "ENOTDIR")
	);
}

async function getMutationQueueKey(filePath: string): Promise<string> {
	const resolvedPath = resolve(filePath);
	try {
		return await realpath(resolvedPath);
	} catch (error) {
		if (isMissingPathError(error)) {
			return resolvedPath;
		}
		throw error;
	}
}

async function getFileIdentity(filePath: string): Promise<string | undefined> {
	try {
		const info = await stat(filePath, { bigint: true });
		return info.ino === 0n ? undefined : `${info.dev}:${info.ino}`;
	} catch (error) {
		if (isMissingPathError(error)) {
			return undefined;
		}
		throw error;
	}
}

/**
 * Serialize file mutation operations targeting the same file.
 * Operations for different files still run in parallel.
 */
export async function withFileMutationQueue<T>(filePath: string, fn: () => Promise<T>): Promise<T> {
	const registration = registrationQueue.then(async () => {
		const key = await getMutationQueueKey(filePath);
		const identity = await getFileIdentity(key);
		const predecessors: Promise<void>[] = [];
		for (const [path, entry] of fileMutationQueues) {
			if (path === key) {
				predecessors.push(entry.queue);
			} else if (identity !== undefined) {
				// A queued write may have created a file that was missing at registration.
				entry.identity ??= await getFileIdentity(path).catch(() => undefined);
				if (entry.identity === identity) {
					predecessors.push(entry.queue);
				}
			}
		}
		const currentQueue = Promise.all(predecessors).then(() => undefined);

		let releaseNext!: () => void;
		const nextQueue = new Promise<void>((resolveQueue) => {
			releaseNext = resolveQueue;
		});
		const chainedQueue = currentQueue.then(() => nextQueue);
		const entry = { queue: chainedQueue, identity };
		fileMutationQueues.set(key, entry);

		return { key, currentQueue, entry, releaseNext };
	});
	registrationQueue = registration.then(
		() => undefined,
		() => undefined,
	);

	const { key, currentQueue, entry, releaseNext } = await registration;
	await currentQueue;
	try {
		return await fn();
	} finally {
		releaseNext();
		if (fileMutationQueues.get(key) === entry) {
			fileMutationQueues.delete(key);
		}
	}
}
