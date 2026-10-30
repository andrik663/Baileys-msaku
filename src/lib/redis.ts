import logger from "@/lib/logger";

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || "https://internal-salmon-111730.upstash.io";
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || "gQAAAAAAAbRyAAIgcDJhM2RhOWY5OTJmNzc0Mzk0YmEwNzQ3ZWVhMTUyYzE0YQ";

async function upstashCmd(command: unknown[]): Promise<any> {
  const res = await fetch(UPSTASH_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(command),
  });
  if (!res.ok) {
    throw new Error(`Upstash HTTP ${res.status}: ${await res.text()}`);
  }
  const data = (await res.json()) as { result?: any; error?: string };
  if (data.error) throw new Error(data.error);
  return data.result;
}

async function upstashPipeline(commands: unknown[][]): Promise<any[]> {
  if (commands.length === 0) return [];
  const res = await fetch(`${UPSTASH_URL}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(commands),
  });
  if (!res.ok) {
    throw new Error(`Upstash Pipeline HTTP ${res.status}: ${await res.text()}`);
  }
  const data = (await res.json()) as Array<{ result?: any; error?: string }>;
  return data.map((d) => {
    if (d.error) throw new Error(d.error);
    return d.result;
  });
}

const redis = {
  isOpen: true,
  on: (_event: string, _cb: (...args: any[]) => void) => redis,
  connect: async () => {
    logger.info("Connected to Upstash HTTP Redis");
    return redis;
  },
  clientSetName: async (_name: string) => "OK",
  get: async (key: string): Promise<string | null> => {
    const r = await upstashCmd(["GET", key]);
    return r ?? null;
  },
  set: async (key: string, value: string, opts?: { NX?: boolean; EX?: number }): Promise<string | null> => {
    const cmd: unknown[] = ["SET", key, value];
    if (opts?.EX) cmd.push("EX", opts.EX);
    if (opts?.NX) cmd.push("NX");
    return await upstashCmd(cmd);
  },
  del: async (key: string): Promise<number> => {
    return (await upstashCmd(["DEL", key])) ?? 0;
  },
  keys: async (pattern: string): Promise<string[]> => {
    return (await upstashCmd(["KEYS", pattern])) ?? [];
  },
  hSet: async (key: string, field: string, value: string): Promise<number> => {
    return (await upstashCmd(["HSET", key, field, value])) ?? 0;
  },
  hGet: async (key: string, field: string): Promise<string | null> => {
    const r = await upstashCmd(["HGET", key, field]);
    return r ?? null;
  },
  hDel: async (key: string, field: string): Promise<number> => {
    return (await upstashCmd(["HDEL", key, field])) ?? 0;
  },
  multi: () => {
    const cmds: unknown[][] = [];
    const chain = {
      hSet: (key: string, field: string, value: string) => {
        cmds.push(["HSET", key, field, value]);
        return chain;
      },
      hGet: (key: string, field: string) => {
        cmds.push(["HGET", key, field]);
        return chain;
      },
      hDel: (key: string, field: string) => {
        cmds.push(["HDEL", key, field]);
        return chain;
      },
      execAsPipeline: async () => {
        return await upstashPipeline(cmds);
      },
    };
    return chain;
  },
};

export async function initializeRedis() {
  await redis.connect();
  return redis;
}

export default redis;
