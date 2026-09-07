"""Read-only, bounded console evidence. Never print credentials or raw research."""
import json
from pathlib import Path
import urllib.request
from liangjian_funnel.settings import load_dotenv


def main():
    env = load_dotenv(Path(".env"))
    token = env.get("LIANGJIAN_DASHBOARD_TOKEN", "")
    def get(path):
        request = urllib.request.Request("http://127.0.0.1:3210" + path,
                                         headers={"Authorization": "Bearer " + token})
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)
    data = get("/api/overview")
    workflow = data.get("latestWorkflow") or {}
    monitor = data.get("monitor") or {}
    result = {"generatedAt": data.get("generatedAt"), "activeJob": data.get("activeJob"),
              "run": {key: workflow.get(key) for key in ("runId", "tradeDate", "status")},
              "planCounts": data.get("planCounts"), "businessHealth": data.get("businessHealth"),
              "decisionData": data.get("decisionData"),
              "planSample": (monitor.get("latestA3Plans") or [])[:1],
              "reviewSample": [{k: r.get(k) for k in ("reviewId", "tradeDate", "status", "cutoffAt")}
                               for r in (data.get("recentA5Reviews") or [])[:2]],
              "notificationSample": (monitor.get("notifications") or [])[:2],
              "sources": data.get("dataSources"), "stages": []}
    for lane in workflow.get("lanes") or []:
        if lane.get("laneId") != "lane_1":
            continue
        for stage in lane.get("stages") or []:
            name = stage.get("stage")
            detail = get(f'/api/research/runs/{workflow["runId"]}/lanes/lane_1/stages/{name}?pageSize=1')
            result["stages"].append({"stage": name, "outcome": stage.get("outcome"),
                "inputCount": detail.get("inputCount"), "outputCount": detail.get("outputCount"),
                "pools": detail.get("pools"), "plan": (detail.get("items") or [{}])[0].get("plan")})
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
