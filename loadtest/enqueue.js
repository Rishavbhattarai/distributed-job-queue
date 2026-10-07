// k6 enqueue load. Two modes (env MODE):
//   burst:  JOBS jobs as fast as VUS virtual users can POST them (drain/throughput test)
//   steady: RATE jobs/s for DURATION seconds (latency test, below saturation)
// Every job carries {"run": RUN} in its payload so measure.sql can select one run.
import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE || "http://api:8000";
const MODE = __ENV.MODE || "burst";
const RUN = __ENV.RUN || "manual";
const JOB_TYPE = __ENV.JOB_TYPE || "echo";
const SECONDS = parseFloat(__ENV.JOB_SECONDS || "0");

export const options = {
  scenarios:
    MODE === "steady"
      ? {
          steady: {
            executor: "constant-arrival-rate",
            rate: parseInt(__ENV.RATE || "100"),
            timeUnit: "1s",
            duration: `${__ENV.DURATION || "30"}s`,
            preAllocatedVUs: 50,
            maxVUs: 200,
          },
        }
      : {
          burst: {
            executor: "shared-iterations",
            vus: parseInt(__ENV.VUS || "50"),
            iterations: parseInt(__ENV.JOBS || "10000"),
            maxDuration: "10m",
          },
        },
  summaryTrendStats: ["avg", "p(50)", "p(99)", "max"],
};

const params = { headers: { "Content-Type": "application/json" } };

export default function () {
  const payload = JOB_TYPE === "sleep" ? { run: RUN, seconds: SECONDS } : { run: RUN };
  const res = http.post(`${BASE}/jobs`, JSON.stringify({ type: JOB_TYPE, payload }), params);
  check(res, { "201 created": (r) => r.status === 201 });
}
