// k6 load test for POST /api/sales/{id}/buy, run on the compose network (see loadtest/run_k6.ps1).
//
//   SCENARIO=throughput  constant 5,000 req/s for 60 s against a 1,000,000-unit sale:
//                        every request is a real write (Redis reserve + Kafka + PostgreSQL).
//   SCENARIO=hot_item    5,000 req/s for 20 s against a 1,000-unit sale: proves zero oversell.
//
// JWTs are signed here with the shared HS256 key, one unique user per request.
import http from "k6/http";
import { check } from "k6";
import crypto from "k6/crypto";
import encoding from "k6/encoding";
import exec from "k6/execution";
import { Counter } from "k6/metrics";

const SCENARIO = __ENV.SCENARIO || "throughput";
const BASE_URL = __ENV.BASE_URL || "http://nginx";
const SALE_ID = __ENV.SALE_ID;
const FIRST_USER_ID = Number(__ENV.FIRST_USER_ID);
const KEY = __ENV.JWT_SIGNING_KEY;
const RATE = Number(__ENV.RATE || 5000);
const DURATION = __ENV.DURATION || (SCENARIO === "hot_item" ? "20s" : "60s");

if (!SALE_ID || !FIRST_USER_ID || !KEY) {
  throw new Error("SALE_ID, FIRST_USER_ID and JWT_SIGNING_KEY are required");
}

const reserved = new Counter("reserved");     // 202
const soldOut = new Counter("sold_out");      // 409 sold_out
const busy = new Counter("try_again");        // 503 (Kafka slow/unavailable)

export const options = {
  scenarios: {
    [SCENARIO]: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: DURATION,
      preAllocatedVUs: Number(__ENV.VUS || 500),
      maxVUs: Number(__ENV.MAX_VUS || 3000),
    },
  },
  thresholds:
    SCENARIO === "throughput"
      ? {
          "http_req_duration{scenario:throughput}": ["p(99)<50"],
          "http_req_failed{scenario:throughput}": ["rate<0.01"],
          dropped_iterations: ["count<1"],
        }
      : {
          "http_req_failed{scenario:hot_item}": ["rate<0.01"],
        },
  summaryTrendStats: ["avg", "med", "p(95)", "p(99)", "max"],
};

// In the hot-item run most buyers are *supposed* to get 409 sold_out.
http.setResponseCallback(SCENARIO === "hot_item" ? http.expectedStatuses(202, 409) : http.expectedStatuses(202));

const b64url = (obj) => encoding.b64encode(JSON.stringify(obj), "rawurl");
const HEADER = b64url({ alg: "HS256", typ: "JWT" });

function token(userId) {
  const now = Math.floor(Date.now() / 1000);
  const payload = b64url({ user_id: userId, token_type: "access", iat: now, exp: now + 3600 });
  const signature = crypto.hmac("sha256", KEY, `${HEADER}.${payload}`, "base64rawurl");
  return `${HEADER}.${payload}.${signature}`;
}

export default function () {
  const userId = FIRST_USER_ID + exec.scenario.iterationInTest;
  const res = http.post(`${BASE_URL}/api/sales/${SALE_ID}/buy`, null, {
    headers: { Authorization: `Bearer ${token(userId)}` },
    tags: { name: "buy" },
  });

  if (res.status === 202) reserved.add(1);
  else if (res.status === 409) soldOut.add(1);
  else if (res.status === 503) busy.add(1);

  if (SCENARIO === "hot_item") {
    check(res, { "202 or 409 sold_out": (r) => r.status === 202 || r.status === 409 });
  } else {
    check(res, { "202 reserved": (r) => r.status === 202 });
  }
}
