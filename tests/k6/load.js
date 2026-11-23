import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const BASE = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const POLICY = __ENV.ROUTING_POLICY || 'smart';
const TOKENS = Number(__ENV.MAX_TOKENS || 24);

// 80% of requests share a prefix: this is what makes KV-cache affinity matter.
const SHARED_PREFIX =
  'You are a helpful assistant for a distributed inference gateway. ' +
  'Explain how consistent hashing, prefix caching, load shedding and circuit breakers work. '.repeat(8);

export const options = {
  scenarios: {
    load: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 20 },
        { duration: '30s', target: 100 },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '2s',
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<5000'],
    http_req_failed: ['rate<0.60'], // shedding is intentional under overload
    checks: ['rate>0.40'],
  },
};

export default function () {
  const shared = Math.random() < 0.8;
  const prompt = shared
    ? SHARED_PREFIX
    : `Unique request ${exec.scenario.iterationInTest}-${Math.random()}`;

  const payload = JSON.stringify({
    model: 'workhorse',
    messages: [{ role: 'user', content: prompt }],
    max_tokens: TOKENS,
    temperature: 0,
    stream: true,
  });

  const res = http.post(`${BASE}/v1/chat/completions`, payload, {
    headers: {
      'Content-Type': 'application/json',
      Authorization: 'Bearer dev-key',
      'X-Routing-Policy': POLICY,
    },
    tags: { policy: POLICY, prefix: shared ? 'shared' : 'unique' },
    timeout: '30s',
  });

  check(res, {
    'status is 200': (r) => r.status === 200,
    'stream has DONE': (r) => String(r.body).includes('[DONE]'),
  });
}
