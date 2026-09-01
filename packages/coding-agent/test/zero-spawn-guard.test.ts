import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentSession, referencesPendingChildWork } from "../src/core/agent-session.js";

// Lightweight harness in the goal-continuation-quiescence.test.ts style:
// private-method access via Reflect with a fake `this`, no full session.
type Harness = {
	_rlmDepth: number;
	_zeroSpawnGuardFired: boolean;
	_rlmChildSessions: Map<string, unknown>;
	_activeRlmChildRuns: Map<string, unknown>;
	_abandonedRlmQuiescenceChildIds: Set<string>;
	_rlmChildSessionSnapshot: () => unknown[];
	_queuePreparedPrompt: ReturnType<typeof vi.fn>;
};

const maybeFire = Reflect.get(AgentSession.prototype, "_maybeFireZeroSpawnGuard") as (
	this: Harness,
	message: { role: string; content: unknown },
) => Promise<void>;

function harness(overrides: Partial<Harness> = {}): Harness {
	return {
		_rlmDepth: 0,
		_zeroSpawnGuardFired: false,
		_rlmChildSessions: new Map(),
		_activeRlmChildRuns: new Map(),
		_abandonedRlmQuiescenceChildIds: new Set(),
		// Mirrors the real snapshot semantics: registered sessions minus
		// abandoned ids, plus non-abandoned active runs.
		_rlmChildSessionSnapshot(this: Harness) {
			return [...this._rlmChildSessions.values()];
		},
		_queuePreparedPrompt: vi.fn().mockResolvedValue(true),
		...overrides,
	};
}

const HANG_MSG = {
	role: "assistant",
	content: [
		{
			type: "text",
			text: "Analysis is still running. I'll synthesize the four independent reviews when they return.",
		},
	],
};
const HANG_MSG_2 = {
	role: "assistant",
	content: [
		{
			type: "text",
			text: "The final synthesis will include the exact worker counts, model routing, and rollout metrics once the independent reviews finish.",
		},
	],
};
const PLAIN_MSG = {
	role: "assistant",
	content: [{ type: "text", text: "Done. The tests pass and the report is written." }],
};

describe("zero-spawn guard heuristic", () => {
	it("matches both verbatim sentences from the 2026-09-01 hang", () => {
		expect(referencesPendingChildWork(HANG_MSG.content as never)).toBe(true);
		expect(referencesPendingChildWork(HANG_MSG_2.content as never)).toBe(true);
	});

	it("does not match ordinary completion text", () => {
		const text = (s: string) => [{ type: "text", text: s }] as never;
		expect(referencesPendingChildWork(PLAIN_MSG.content as never)).toBe(false);
		expect(referencesPendingChildWork(text("I ran the tests and they pass."))).toBe(false);
		expect(referencesPendingChildWork(text("I'll synthesize the report now."))).toBe(false);
		expect(referencesPendingChildWork(text("The build will run when the timer fires."))).toBe(false);
	});
});

describe("zero-spawn guard firing", () => {
	afterEach(() => {
		delete process.env.PRIME_AGENT_ZERO_SPAWN_GUARD;
	});

	it("queues an idle-resuming steer prompt when a root parks on phantom children", async () => {
		const mode = harness();
		await maybeFire.call(mode, HANG_MSG as never);
		expect(mode._queuePreparedPrompt).toHaveBeenCalledTimes(1);
		const [schedule, text, images, options] = mode._queuePreparedPrompt.mock.calls[0]!;
		expect(schedule).toBe("steer");
		expect(text).toContain("[zero-spawn guard]");
		expect(text).toContain("rlm.list_subagents()");
		expect(images).toBeUndefined();
		expect((options as { resumeIfIdle: boolean }).resumeIfIdle).toBe(true);
		expect(mode._zeroSpawnGuardFired).toBe(true);
	});

	it("fires at most once per user prompt (the latch)", async () => {
		const mode = harness();
		await maybeFire.call(mode, HANG_MSG as never);
		await maybeFire.call(mode, HANG_MSG as never);
		expect(mode._queuePreparedPrompt).toHaveBeenCalledTimes(1);
	});

	it("re-arms when the latch is cleared (a new user prompt resets it)", async () => {
		const mode = harness();
		await maybeFire.call(mode, HANG_MSG as never);
		mode._zeroSpawnGuardFired = false; // what _prompt does on the next user message
		await maybeFire.call(mode, HANG_MSG as never);
		expect(mode._queuePreparedPrompt).toHaveBeenCalledTimes(2);
	});

	it("never fires inside rlm child sessions", async () => {
		const mode = harness({ _rlmDepth: 1 });
		await maybeFire.call(mode, HANG_MSG as never);
		expect(mode._queuePreparedPrompt).not.toHaveBeenCalled();
	});

	it("stays silent while live children may still reply", async () => {
		const mode = harness({ _rlmChildSessions: new Map([["sub-1", {}]]) });
		await maybeFire.call(mode, HANG_MSG as never);
		expect(mode._queuePreparedPrompt).not.toHaveBeenCalled();
	});

	it("stays silent for ordinary final messages", async () => {
		const mode = harness();
		await maybeFire.call(mode, PLAIN_MSG as never);
		expect(mode._queuePreparedPrompt).not.toHaveBeenCalled();
	});

	it("is disabled by PRIME_AGENT_ZERO_SPAWN_GUARD=0", async () => {
		process.env.PRIME_AGENT_ZERO_SPAWN_GUARD = "0";
		const mode = harness();
		await maybeFire.call(mode, HANG_MSG as never);
		expect(mode._queuePreparedPrompt).not.toHaveBeenCalled();
	});
});
